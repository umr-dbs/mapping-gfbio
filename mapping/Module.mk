
#
# Core libraries
#
PKGLIBS_CORE += jsoncpp gdal libpng zlib "libpqxx >= 4.0.0" libcurl sqlite3 libarchive
#
# Core objects
#
OBJS_CORE += o/datatypes/attributes.o o/datatypes/spatiotemporal.o o/datatypes/raster/import_gdal.o o/datatypes/raster/export_pgm.o o/datatypes/raster/export_yuv.o o/datatypes/raster/export_png.o o/datatypes/raster/export_jpeg.o o/datatypes/simplefeaturecollection.o o/datatypes/pointcollection.o o/datatypes/linecollection.o o/datatypes/polygoncollection.o o/datatypes/simplefeaturecollections/geosgeomutil.o o/datatypes/simplefeaturecollections/wkbutil.o o/datatypes/unit.o o/datatypes/colorizer.o
OBJS_CORE_WITHCL += o/datatypes/raster/raster.o o/raster/opencl.o
OBJS_CORE_NOCL += o/datatypes/raster/raster_nocl.o
OBJS_CORE += o/raster/profiler.o
# rasterdb: move to core for now
OBJS_CORE += o/rasterdb/rasterdb.o o/rasterdb/backend.o o/rasterdb/backend_local.o o/rasterdb/backend_remote.o
OBJS_CORE += o/rasterdb/converters/converter.o o/rasterdb/converters/raw.o
# userdb: move to core for now
OBJS_CORE += o/userdb/userdb.o o/userdb/backend_sqlite.o
OBJS_CORE += o/util/gdal.o o/util/sha1.o o/util/curl.o o/util/sqlite.o o/util/binarystream.o o/util/csvparser.o o/util/base64.o o/util/configuration.o o/util/formula.o o/util/debug.o o/util/timemodification.o o/util/log.o o/util/timeparser.o o/util/sizeutil.o
# operator and query base must be in core for now
OBJS_CORE += o/operators/operator.o o/operators/provenance.o o/operators/queryrectangle.o o/operators/queryprofiler.o

#
# HTTP Services
#
OBJS_SERVICES += o/services/httpservice.o o/services/httpparsing.o o/services/user.o o/services/ogcservice.o o/services/wms.o o/services/wcs.o o/services/wfs.o o/services/plot.o o/services/provenance.o o/services/artifact.o o/services/gfbio.o
# pointvisualization is only used by services, so that's where it goes
OBJS_SERVICES += o/pointvisualization/BoundingBox.o o/pointvisualization/Circle.o o/pointvisualization/Coordinate.o o/pointvisualization/Dimension.o o/pointvisualization/FindResult.o o/pointvisualization/QuadTreeNode.o o/pointvisualization/CircleClusteringQuadTree.o

#
# Operators
#
OBJS_OPERATORS += o/operators/source/csv_source.o o/operators/source/gfbio_source.o o/operators/source/pangaea_source.o o/operators/source/postgres_source.o o/operators/source/rasterdb_source.o o/operators/source/wkt_source.o o/operators/source/gbif_source.o
OBJS_OPERATORS += o/operators/processing/raster/matrixkernel.o o/operators/processing/raster/expression.o o/operators/processing/raster/classification.o
OBJS_OPERATORS += o/operators/processing/features/difference.o o/operators/processing/features/numeric_attribute_filter.o o/operators/processing/features/point_in_polygon_filter.o
OBJS_OPERATORS += o/operators/processing/combined/projection.o o/operators/processing/combined/raster_value_extraction.o o/operators/processing/combined/rasterization.o o/operators/processing/combined/timeshift.o
OBJS_OPERATORS += o/operators/processing/meteosat/temperature.o o/operators/processing/meteosat/reflectance.o o/operators/processing/meteosat/solarangle.o o/operators/processing/meteosat/radiance.o o/operators/processing/meteosat/pansharpening.o o/operators/processing/meteosat/gccthermthresholddetection.o o/operators/processing/meteosat/co2correction.o
OBJS_OPERATORS += o/util/sunpos.o
OBJS_OPERATORS += o/operators/plots/histogram.o o/operators/plots/feature_attributes_plot.o
OBJS_OPERATORS += o/datatypes/plots/histogram.o o/datatypes/plots/text.o o/datatypes/plots/png.o

# TODO: move to gfbio module
ifeq (${USE_ABCD},true)
	OBJS_OPERATORS += o/operators/source/abcd_source.o
	PKGLIBS_CORE += xerces-c
endif

# Cache: needs to be core for now
OBJS_CORE += o/cache/common.o o/cache/priv/shared.o o/cache/priv/requests.o o/cache/priv/connection.o o/cache/priv/redistribution.o o/cache/priv/cache_stats.o o/cache/priv/cache_structure.o o/cache/node/node_cache.o o/cache/manager.o o/cache/priv/caching_strategy.o o/cache/priv/cube.o

#
# Helper variables
#
OBJ_NODE_SERVER = o/cache/node/node_config.o o/cache/node/nodeserver.o o/cache/node/delivery.o o/cache/node/node_manager.o o/cache/node/manager/local_manager.o o/cache/node/manager/local_replacement.o o/cache/node/manager/remote_manager.o o/cache/node/manager/hybrid_manager.o o/cache/node/puzzle_util.o
OBJ_INDEX_SERVER = o/cache/index/index_config.o o/cache/index/node.o o/cache/index/index_cache_manager.o o/cache/index/indexserver.o o/cache/index/querymanager.o o/cache/index/query_manager/default_query_manager.o o/cache/index/query_manager/simple_query_manager.o o/cache/index/query_manager/emkde_query_manager.o o/cache/index/query_manager/late_query_manager.o o/cache/index/index_cache.o o/cache/index/reorg_strategy.o
OBJ_CACHE_EXP = o/cache/experiments/exp_util.o o/cache/experiments/cache_experiments.o
OBJ_CACHE_EXP += o/operators/processing/combined/projection.o o/operators/processing/combined/timeshift.o

#
# GTest
#
SRC_GTEST:=$(wildcard test/unittests/*.cpp) $(wildcard test/unittests/*/*.cpp)
OBJS_GTEST += $(patsubst test/%,o/test/%,$(SRC_GTEST:.cpp=.o))
# and a few requirements
OBJS_GTEST += o/util/server_nonblocking.o
# TODO: these dependencies are ridiculous and need to be cleaned up
OBJS_GTEST += ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} 




# now define all binaries we wish to create
ALL_TARGETS += MANAGER MANAGER_SAN CGI GTEST PARSETESTLOGS TILESERVER

# Note: make sure to understand the difference between A := B and A = B
# see https://www.gnu.org/software/make/manual/html_node/Flavors.html
TARGET_MANAGER_NAME := mapping_manager 
TARGET_MANAGER_OBJS = o/mapping_manager.o ${OBJS_CORE}
TARGET_MANAGER_PKGLIBS = ${PKGLIBS_CORE}
TARGET_MANAGER_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL}

TARGET_MANAGER_SAN_NAME := mapping_manager.san
TARGET_MANAGER_SAN_OBJS = $(patsubst %.o,%.san.o,${TARGET_MANAGER_OBJS})
TARGET_MANAGER_SAN_PKGLIBS = ${PKGLIBS_CORE}
TARGET_MANAGER_SAN_LDFLAGS = ${SANITIZE_FLAGS} ${LDFLAGS} ${LDFLAGS_CL}

TARGET_CGI_NAME := ../htdocs/cgi-bin/mapping 
TARGET_CGI_OBJS = o/cgi.o ${OBJS_CORE} ${OBJS_SERVICES}
TARGET_CGI_PKGLIBS = ${PKGLIBS_CORE}
TARGET_CGI_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL} -lfcgi++ -lfcgi

TARGET_GTEST_NAME := gtest 
TARGET_GTEST_OBJS = ${OBJS_CORE} ${OBJS_SERVICES} ${OBJS_GTEST} o/libgtest.a
TARGET_GTEST_PKGLIBS = ${PKGLIBS_CORE}
TARGET_GTEST_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL} -lpthread

TARGET_PARSETESTLOGS_NAME := test/systemtests/parse_logs
TARGET_PARSETESTLOGS_OBJS = o/test/systemtests/parse_logs.o
# TODO: this does not need any pkglibs, but pkg-config does not like empty strings
TARGET_PARSETESTLOGS_PKGLIBS = zlib
TARGET_PARSETESTLOGS_LDFLAGS = 

TARGET_TILESERVER_NAME := tileserver
TARGET_TILESERVER_OBJS = o/rasterdb/tileserver.o o/util/server_nonblocking.o o/rasterdb/backend.o o/rasterdb/backend_local.o o/datatypes/attributes.o o/datatypes/unit.o o/util/binarystream.o o/util/sha1.o o/util/sqlite.o o/util/configuration.o o/util/sizeutil.o o/util/log.o
TARGET_TILESERVER_PKGLIBS = ${PKGLIBS_CORE}
TARGET_TILESERVER_LDFLAGS = ${LDFLAGS}


# cache stuff, should probably go into modules mapping-distributed and mapping-playground
ALL_TARGETS += CACHEINDEX CACHENODE CACHEEXPERIMENTS CACHECLUSTEREXPERIMENTS LOCALSETUP TESTCLIENT

TARGET_CACHEINDEX_NAME := cache_index
TARGET_CACHEINDEX_OBJS = ${OBJS_CORE_NOCL} ${OBJ_INDEX_SERVER} o/cache/index/indexserver_main.o o/raster/profiler.o
TARGET_CACHEINDEX_PKGLIBS = ${PKGLIBS_CORE}
TARGET_CACHEINDEX_LDFLAGS = ${LDFLAGS}

TARGET_CACHENODE_NAME := cache_node
TARGET_CACHENODE_OBJS = ${OBJS_CORE} ${OBJ_NODE_SERVER} o/cache/node/nodeserver_main.o
TARGET_CACHENODE_PKGLIBS = ${PKGLIBS_CORE}
TARGET_CACHENODE_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL}

TARGET_CACHEEXPERIMENTS_NAME := cache_experiments
TARGET_CACHEEXPERIMENTS_OBJS = ${OBJS_CORE} ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} o/cache/experiments/cache_experiments_main.o
TARGET_CACHEEXPERIMENTS_PKGLIBS = ${PKGLIBS_CORE}
TARGET_CACHEEXPERIMENTS_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL}

TARGET_CACHECLUSTEREXPERIMENTS_NAME := cluster_experiment
TARGET_CACHECLUSTEREXPERIMENTS_OBJS = ${OBJS_CORE} ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} o/cache/experiments/cluster_experiment.o
TARGET_CACHECLUSTEREXPERIMENTS_PKGLIBS = ${PKGLIBS_CORE}
TARGET_CACHECLUSTEREXPERIMENTS_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL}

TARGET_LOCALSETUP_NAME := local_setup
TARGET_LOCALSETUP_OBJS = ${OBJS_CORE} ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} o/cache/experiments/local_setup.o
TARGET_LOCALSETUP_PKGLIBS = ${PKGLIBS_CORE}
TARGET_LOCALSETUP_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL}

TARGET_TESTCLIENT_NAME := test_client
TARGET_TESTCLIENT_OBJS = ${OBJS_CORE} ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} o/cache/experiments/nbclient.o o/services/httpservice.o o/services/httpparsing.o o/services/ogcservice.o
TARGET_TESTCLIENT_PKGLIBS = ${PKGLIBS_CORE}
TARGET_TESTCLIENT_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL}
