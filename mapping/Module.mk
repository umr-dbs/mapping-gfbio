
#
# Core libraries
#
PKGLIBS_CORE += jsoncpp gdal libpng zlib "libpqxx >= 4.0.0" libcurl sqlite3 libarchive
#
# Core objects
#
OBJS_CORE += o/core/datatypes/attributes.o o/core/datatypes/spatiotemporal.o o/core/datatypes/raster/import_gdal.o o/core/datatypes/raster/export_pgm.o o/core/datatypes/raster/export_yuv.o o/core/datatypes/raster/export_png.o o/core/datatypes/raster/export_jpeg.o o/core/datatypes/simplefeaturecollection.o o/core/datatypes/pointcollection.o o/core/datatypes/linecollection.o o/core/datatypes/polygoncollection.o o/core/datatypes/simplefeaturecollections/geosgeomutil.o o/core/datatypes/simplefeaturecollections/wkbutil.o o/core/datatypes/unit.o o/core/datatypes/colorizer.o
OBJS_CORE_WITHCL += o/core/datatypes/raster/raster.o o/core/raster/opencl.o
OBJS_CORE_NOCL += o/core/datatypes/raster/raster_nocl.o
OBJS_CORE += o/core/raster/profiler.o
# rasterdb: move to core for now
OBJS_CORE += o/core/rasterdb/rasterdb.o o/core/rasterdb/backend.o o/core/rasterdb/backend_local.o o/core/rasterdb/backend_remote.o
OBJS_CORE += o/core/rasterdb/converters/converter.o o/core/rasterdb/converters/raw.o
# userdb: move to core for now
OBJS_CORE += o/core/userdb/userdb.o o/core/userdb/backend_sqlite.o
OBJS_CORE += o/core/util/gdal.o o/core/util/sha1.o o/core/util/curl.o o/core/util/sqlite.o o/core/util/binarystream.o o/core/util/csvparser.o o/core/util/base64.o o/core/util/configuration.o o/core/util/formula.o o/core/util/debug.o o/core/util/timemodification.o o/core/util/log.o o/core/util/timeparser.o o/core/util/sizeutil.o
# operator and query base must be in core for now
OBJS_CORE += o/core/operators/operator.o o/core/operators/provenance.o o/core/operators/queryrectangle.o o/core/operators/queryprofiler.o

#
# HTTP Services
#
OBJS_SERVICES += o/core/services/httpservice.o o/core/services/httpparsing.o o/core/services/user.o o/core/services/ogcservice.o o/core/services/wms.o o/core/services/wcs.o o/core/services/wfs.o o/core/services/plot.o o/core/services/provenance.o o/core/services/artifact.o o/core/services/gfbio.o
# pointvisualization is only used by services, so that's where it goes
OBJS_SERVICES += o/core/pointvisualization/BoundingBox.o o/core/pointvisualization/Circle.o o/core/pointvisualization/Coordinate.o o/core/pointvisualization/Dimension.o o/core/pointvisualization/FindResult.o o/core/pointvisualization/QuadTreeNode.o o/core/pointvisualization/CircleClusteringQuadTree.o

#
# Operators
#
OBJS_OPERATORS += o/core/operators/source/csv_source.o o/core/operators/source/gfbio_source.o o/core/operators/source/pangaea_source.o o/core/operators/source/postgres_source.o o/core/operators/source/rasterdb_source.o o/core/operators/source/wkt_source.o o/core/operators/source/gbif_source.o
OBJS_OPERATORS += o/core/operators/processing/raster/matrixkernel.o o/core/operators/processing/raster/expression.o o/core/operators/processing/raster/classification.o
OBJS_OPERATORS += o/core/operators/processing/features/difference.o o/core/operators/processing/features/numeric_attribute_filter.o o/core/operators/processing/features/point_in_polygon_filter.o
OBJS_OPERATORS += o/core/operators/processing/combined/projection.o o/core/operators/processing/combined/raster_value_extraction.o o/core/operators/processing/combined/rasterization.o o/core/operators/processing/combined/timeshift.o
OBJS_OPERATORS += o/core/operators/processing/meteosat/temperature.o o/core/operators/processing/meteosat/reflectance.o o/core/operators/processing/meteosat/solarangle.o o/core/operators/processing/meteosat/radiance.o o/core/operators/processing/meteosat/pansharpening.o o/core/operators/processing/meteosat/gccthermthresholddetection.o o/core/operators/processing/meteosat/co2correction.o
OBJS_OPERATORS += o/core/util/sunpos.o
OBJS_OPERATORS += o/core/operators/plots/histogram.o o/core/operators/plots/feature_attributes_plot.o
OBJS_OPERATORS += o/core/datatypes/plots/histogram.o o/core/datatypes/plots/text.o o/core/datatypes/plots/png.o

# TODO: move to gfbio module
ifeq (${USE_ABCD},true)
	OBJS_OPERATORS += o/core/operators/source/abcd_source.o
	PKGLIBS_CORE += xerces-c
endif

# Cache: needs to be core for now
OBJS_CORE += o/core/cache/common.o o/core/cache/priv/shared.o o/core/cache/priv/requests.o o/core/cache/priv/connection.o o/core/cache/priv/redistribution.o o/core/cache/priv/cache_stats.o o/core/cache/priv/cache_structure.o o/core/cache/node/node_cache.o o/core/cache/manager.o o/core/cache/priv/caching_strategy.o o/core/cache/priv/cube.o

#
# Helper variables
#
OBJ_NODE_SERVER = o/core/cache/node/node_config.o o/core/cache/node/nodeserver.o o/core/cache/node/delivery.o o/core/cache/node/node_manager.o o/core/cache/node/manager/local_manager.o o/core/cache/node/manager/local_replacement.o o/core/cache/node/manager/remote_manager.o o/core/cache/node/manager/hybrid_manager.o o/core/cache/node/puzzle_util.o
OBJ_INDEX_SERVER = o/core/cache/index/index_config.o o/core/cache/index/node.o o/core/cache/index/index_cache_manager.o o/core/cache/index/indexserver.o o/core/cache/index/querymanager.o o/core/cache/index/query_manager/default_query_manager.o o/core/cache/index/query_manager/simple_query_manager.o o/core/cache/index/query_manager/emkde_query_manager.o o/core/cache/index/query_manager/late_query_manager.o o/core/cache/index/index_cache.o o/core/cache/index/reorg_strategy.o
OBJ_CACHE_EXP = o/core/cache/experiments/exp_util.o o/core/cache/experiments/cache_experiments.o
OBJ_CACHE_EXP += o/core/operators/processing/combined/projection.o o/core/operators/processing/combined/timeshift.o

#
# GTest
#
SRC_GTEST:=$(wildcard test/unittests/*.cpp) $(wildcard test/unittests/*/*.cpp)
OBJS_GTEST += $(patsubst test/%,o/core/test/%,$(SRC_GTEST:.cpp=.o))
# and a few requirements
OBJS_GTEST += o/core/util/server_nonblocking.o
# TODO: these dependencies are ridiculous and need to be cleaned up
OBJS_GTEST += ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} 




# now define all binaries we wish to create
ALL_TARGETS += MANAGER MANAGER_SAN CGI GTEST PARSETESTLOGS TILESERVER

# Note: make sure to understand the difference between A := B and A = B
# see https://www.gnu.org/software/make/manual/html_node/Flavors.html
TARGET_MANAGER_NAME := mapping_manager 
TARGET_MANAGER_OBJS = o/core/mapping_manager.o ${OBJS_CORE} ${OBJS_OPERATORS}
TARGET_MANAGER_PKGLIBS = ${PKGLIBS_CORE}
TARGET_MANAGER_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL}

TARGET_MANAGER_SAN_NAME := mapping_manager.san
TARGET_MANAGER_SAN_OBJS = $(patsubst %.o,%.san.o,${TARGET_MANAGER_OBJS})
TARGET_MANAGER_SAN_PKGLIBS = ${PKGLIBS_CORE}
TARGET_MANAGER_SAN_LDFLAGS = ${SANITIZE_FLAGS} ${LDFLAGS} ${LDFLAGS_CL}

TARGET_CGI_NAME := ../htdocs/cgi-bin/mapping 
TARGET_CGI_OBJS = o/core/cgi.o ${OBJS_CORE} ${OBJS_SERVICES} ${OBJS_OPERATORS}
TARGET_CGI_PKGLIBS = ${PKGLIBS_CORE}
TARGET_CGI_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL} -lfcgi++ -lfcgi

TARGET_GTEST_NAME := gtest 
TARGET_GTEST_OBJS = ${OBJS_CORE} ${OBJS_SERVICES} ${OBJS_GTEST} o/core/libgtest.a
TARGET_GTEST_PKGLIBS = ${PKGLIBS_CORE}
TARGET_GTEST_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL} -lpthread

TARGET_PARSETESTLOGS_NAME := test/systemtests/parse_logs
TARGET_PARSETESTLOGS_OBJS = o/core/test/systemtests/parse_logs.o
# TODO: this does not need any pkglibs, but pkg-config does not like empty strings
TARGET_PARSETESTLOGS_PKGLIBS = zlib
TARGET_PARSETESTLOGS_LDFLAGS = 

TARGET_TILESERVER_NAME := tileserver
TARGET_TILESERVER_OBJS = o/core/rasterdb/tileserver.o o/core/util/server_nonblocking.o o/core/rasterdb/backend.o o/core/rasterdb/backend_local.o o/core/datatypes/attributes.o o/core/datatypes/unit.o o/core/util/binarystream.o o/core/util/sha1.o o/core/util/sqlite.o o/core/util/configuration.o o/core/util/sizeutil.o o/core/util/log.o
TARGET_TILESERVER_PKGLIBS = ${PKGLIBS_CORE}
TARGET_TILESERVER_LDFLAGS = ${LDFLAGS}


# cache stuff, should probably go into modules mapping-distributed and mapping-playground
ALL_TARGETS += CACHEINDEX CACHENODE CACHEEXPERIMENTS CACHECLUSTEREXPERIMENTS LOCALSETUP TESTCLIENT

TARGET_CACHEINDEX_NAME := cache_index
TARGET_CACHEINDEX_OBJS = ${OBJS_CORE_NOCL} ${OBJ_INDEX_SERVER} o/core/cache/index/indexserver_main.o o/core/raster/profiler.o
TARGET_CACHEINDEX_PKGLIBS = ${PKGLIBS_CORE}
TARGET_CACHEINDEX_LDFLAGS = ${LDFLAGS}

TARGET_CACHENODE_NAME := cache_node
TARGET_CACHENODE_OBJS = ${OBJS_CORE} ${OBJS_OPERATORS} ${OBJ_NODE_SERVER} o/core/cache/node/nodeserver_main.o
TARGET_CACHENODE_PKGLIBS = ${PKGLIBS_CORE}
TARGET_CACHENODE_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL}

TARGET_CACHEEXPERIMENTS_NAME := cache_experiments
TARGET_CACHEEXPERIMENTS_OBJS = ${OBJS_CORE} ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} o/core/cache/experiments/cache_experiments_main.o
TARGET_CACHEEXPERIMENTS_PKGLIBS = ${PKGLIBS_CORE}
TARGET_CACHEEXPERIMENTS_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL}

TARGET_CACHECLUSTEREXPERIMENTS_NAME := cluster_experiment
TARGET_CACHECLUSTEREXPERIMENTS_OBJS = ${OBJS_CORE} ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} o/core/cache/experiments/cluster_experiment.o
TARGET_CACHECLUSTEREXPERIMENTS_PKGLIBS = ${PKGLIBS_CORE}
TARGET_CACHECLUSTEREXPERIMENTS_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL}

TARGET_LOCALSETUP_NAME := local_setup
TARGET_LOCALSETUP_OBJS = ${OBJS_CORE} ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} o/core/cache/experiments/local_setup.o
TARGET_LOCALSETUP_PKGLIBS = ${PKGLIBS_CORE}
TARGET_LOCALSETUP_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL}

TARGET_TESTCLIENT_NAME := test_client
TARGET_TESTCLIENT_OBJS = ${OBJS_CORE} ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} o/core/cache/experiments/nbclient.o o/core/services/httpservice.o o/core/services/httpparsing.o o/core/services/ogcservice.o
TARGET_TESTCLIENT_PKGLIBS = ${PKGLIBS_CORE}
TARGET_TESTCLIENT_LDFLAGS = ${LDFLAGS} ${LDFLAGS_CL}
