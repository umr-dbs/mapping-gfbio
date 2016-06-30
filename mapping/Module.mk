
# TODO: this requires a better organization and grouping so that other modules can do things like
# OBJ_OPERATORS += o/my_module/operators/my_operator.o

#
# All object files, grouped by subsystem
#
OBJ_SERVICES=o/services/httpservice.o o/services/httpparsing.o o/services/user.o o/services/ogcservice.o o/services/wms.o o/services/wcs.o o/services/wfs.o o/services/plot.o o/services/provenance.o o/services/artifact.o o/services/gfbio.o
OBJ_RASTER=o/raster/opencl.o o/raster/profiler.o
OBJ_RASTERDB=o/rasterdb/rasterdb.o o/rasterdb/backend.o o/rasterdb/backend_local.o o/rasterdb/backend_remote.o
OBJ_USERDB=o/userdb/userdb.o o/userdb/backend_sqlite.o
OBJ_DATATYPES_COMMON=o/datatypes/attributes.o o/datatypes/spatiotemporal.o o/datatypes/raster/import_gdal.o o/datatypes/raster/export_pgm.o o/datatypes/raster/export_yuv.o o/datatypes/raster/export_png.o o/datatypes/raster/export_jpeg.o o/datatypes/simplefeaturecollection.o o/datatypes/pointcollection.o o/datatypes/linecollection.o o/datatypes/polygoncollection.o o/datatypes/simplefeaturecollections/geosgeomutil.o o/datatypes/simplefeaturecollections/wkbutil.o o/datatypes/unit.o o/datatypes/colorizer.o
OBJ_DATATYPES=${OBJ_DATATYPES_COMMON} o/datatypes/raster/raster.o
OBJ_DATATYPES_NOCL=${OBJ_DATATYPES_COMMON} o/datatypes/raster/raster_nocl.o
OBJ_PLOT=o/datatypes/plots/histogram.o o/datatypes/plots/text.o o/datatypes/plots/png.o
OBJ_UTIL=o/util/gdal.o o/util/sha1.o o/util/curl.o o/util/sqlite.o o/util/sunpos.o o/util/binarystream.o o/util/csvparser.o o/util/base64.o o/util/configuration.o o/util/formula.o o/util/debug.o o/util/timemodification.o o/util/log.o o/util/timeparser.o o/util/sizeutil.o
OBJ_POINTVISUALIZATION=o/pointvisualization/BoundingBox.o o/pointvisualization/Circle.o o/pointvisualization/Coordinate.o o/pointvisualization/Dimension.o o/pointvisualization/FindResult.o o/pointvisualization/QuadTreeNode.o o/pointvisualization/CircleClusteringQuadTree.o
OBJ_CONVERTERS=o/rasterdb/converters/converter.o o/rasterdb/converters/raw.o
OBJ_OPERATORS_SOURCE = o/operators/source/csv_source.o o/operators/source/gfbio_source.o o/operators/source/pangaea_source.o o/operators/source/postgres_source.o o/operators/source/rasterdb_source.o o/operators/source/wkt_source.o o/operators/source/gbif_source.o
OBJ_OPERATORS_PROCESSING_RASTER=o/operators/processing/raster/matrixkernel.o o/operators/processing/raster/expression.o o/operators/processing/raster/classification.o
OBJ_OPERATORS_PROCESSING_FEATURES=o/operators/processing/features/difference.o o/operators/processing/features/numeric_attribute_filter.o o/operators/processing/features/point_in_polygon_filter.o
OBJ_OPERATORS_PROCESSING_COMBINED=o/operators/processing/combined/projection.o o/operators/processing/combined/r_script.o o/operators/processing/combined/raster_value_extraction.o o/operators/processing/combined/rasterization.o o/operators/processing/combined/timeshift.o
OBJ_OPERATORS_PROCESSING_METEOSAT=o/operators/processing/meteosat/temperature.o o/operators/processing/meteosat/reflectance.o o/operators/processing/meteosat/solarangle.o o/operators/processing/meteosat/radiance.o o/operators/processing/meteosat/pansharpening.o o/operators/processing/meteosat/gccthermthresholddetection.o o/operators/processing/meteosat/co2correction.o
OBJ_OPERATORS_PLOTS=o/operators/plots/histogram.o o/operators/plots/feature_attributes_plot.o
# TODO: move to gfbio module
ifeq (${USE_ABCD},true)
	OBJ_OPERATORS_SOURCE += o/operators/source/abcd_source.o
	PKGLIBS_CORE += xerces-c
endif
OBJ_OPERATORS_ALL=$(OBJ_OPERATORS_SOURCE) $(OBJ_OPERATORS_PROCESSING_RASTER) $(OBJ_OPERATORS_PROCESSING_FEATURES) $(OBJ_OPERATORS_PROCESSING_COMBINED) $(OBJ_OPERATORS_PROCESSING_METEOSAT) $(OBJ_OPERATORS_PLOTS)

OBJ_OPERATORS_ALLSTUBS=${OBJ_OPERATORS_ALL:o/operators/%=o/operators_stub/%}

OBJ_OPERATORS_COMMON=o/operators/operator.o o/operators/provenance.o o/operators/queryrectangle.o o/operators/queryprofiler.o
OBJ_OPERATORS=${OBJ_OPERATORS_COMMON} ${OBJ_OPERATORS_ALL}
OBJ_OPERATORS_STUBS=${OBJ_OPERATORS_COMMON} ${OBJ_OPERATORS_ALLSTUBS}

OBJ_CACHE_COMMON=o/cache/common.o o/cache/priv/shared.o o/cache/priv/requests.o o/cache/priv/connection.o o/cache/priv/redistribution.o o/cache/priv/cache_stats.o o/cache/priv/cache_structure.o o/cache/node/node_cache.o o/cache/manager.o o/cache/priv/caching_strategy.o o/cache/priv/cube.o
OBJ_COMMON=${OBJ_CACHE_COMMON} ${OBJ_RASTER} ${OBJ_RASTERDB} ${OBJ_DATATYPES} ${OBJ_CONVERTERS} ${OBJ_PLOT} ${OBJ_POINTVISUALIZATION} ${OBJ_OPERATORS} ${OBJ_UTIL}

OBJ_NODE_SERVER=o/cache/node/node_config.o o/cache/node/nodeserver.o o/cache/node/delivery.o o/cache/node/node_manager.o o/cache/node/manager/local_manager.o o/cache/node/manager/local_replacement.o o/cache/node/manager/remote_manager.o o/cache/node/manager/hybrid_manager.o o/cache/node/puzzle_util.o
OBJ_INDEX_SERVER=o/cache/index/index_config.o o/cache/index/node.o o/cache/index/index_cache_manager.o o/cache/index/indexserver.o o/cache/index/querymanager.o o/cache/index/query_manager/default_query_manager.o o/cache/index/query_manager/simple_query_manager.o o/cache/index/query_manager/emkde_query_manager.o o/cache/index/query_manager/late_query_manager.o o/cache/index/index_cache.o o/cache/index/reorg_strategy.o
OBJ_CACHE_EXP=o/cache/experiments/exp_util.o o/cache/experiments/cache_experiments.o

SRC_UNITTEST:=$(wildcard test/unittests/*.cpp) $(wildcard test/unittests/*/*.cpp)
OBJ_UNITTEST:=$(patsubst test/%,o/test/%,$(SRC_UNITTEST:.cpp=.o))

# now define all binaries we wish to create
ALL_TARGETS += MANAGER MANAGER_SAN CGI GTEST PARSETESTLOGS TILESERVER

TARGET_MANAGER_NAME := mapping_manager 
TARGET_MANAGER_OBJS = o/mapping_manager.o ${OBJ_COMMON} ${OBJ_USERDB}
TARGET_MANAGER_PKGLIBS := ${PKGLIBS_CORE}
TARGET_MANAGER_LDFLAGS := ${LDFLAGS} ${LDFLAGS_CL}

TARGET_MANAGER_SAN_NAME := mapping_manager.san
TARGET_MANAGER_SAN_OBJS = $(patsubst %.o,%.san.o,${TARGET_MANAGER_OBJS})
TARGET_MANAGER_SAN_PKGLIBS := ${PKGLIBS_CORE}
TARGET_MANAGER_SAN_LDFLAGS := ${SANITIZE_FLAGS} ${LDFLAGS} ${LDFLAGS_CL}

TARGET_CGI_NAME := ../htdocs/cgi-bin/mapping 
TARGET_CGI_OBJS = o/cgi.o ${OBJ_COMMON} ${OBJ_SERVICES} ${OBJ_USERDB}
TARGET_CGI_PKGLIBS := ${PKGLIBS_CORE}
TARGET_CGI_LDFLAGS := ${LDFLAGS} ${LDFLAGS_CL} -lfcgi++ -lfcgi

TARGET_GTEST_NAME := gtest 
TARGET_GTEST_OBJS = ${OBJ_COMMON} ${OBJ_SERVICES} ${OBJ_USERDB} ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} o/util/server_nonblocking.o ${OBJ_UNITTEST} o/libgtest.a
TARGET_GTEST_PKGLIBS := ${PKGLIBS_CORE}
TARGET_GTEST_LDFLAGS := ${LDFLAGS} ${LDFLAGS_CL} -lpthread

TARGET_PARSETESTLOGS_NAME := test/systemtests/parse_logs
TARGET_PARSETESTLOGS_OBJS = o/test/systemtests/parse_logs.o
TARGET_PARSETESTLOGS_PKGLIBS := ${PKGLIBS_CORE}
TARGET_PARSETESTLOGS_LDFLAGS := 

TARGET_TILESERVER_NAME := tileserver
TARGET_TILESERVER_OBJS = o/rasterdb/tileserver.o o/util/server_nonblocking.o o/rasterdb/backend.o o/rasterdb/backend_local.o o/datatypes/attributes.o o/datatypes/unit.o o/util/binarystream.o o/util/sha1.o o/util/sqlite.o o/util/configuration.o o/util/sizeutil.o o/util/log.o
TARGET_TILESERVER_PKGLIBS := ${PKGLIBS_CORE}
TARGET_TILESERVER_LDFLAGS := ${LDFLAGS}


# cache stuff, should probably go into modules mapping-distributed and mapping-playground
ALL_TARGETS += CACHEINDEX CACHENODE CACHEEXPERIMENTS CACHECLUSTEREXPERIMENTS LOCALSETUP TESTCLIENT

TARGET_CACHEINDEX_NAME := cache_index
TARGET_CACHEINDEX_OBJS = ${OBJ_CACHE_COMMON} ${OBJ_OPERATORS_COMMON} ${OBJ_DATATYPES_NOCL} ${OBJ_UTIL} ${OBJ_INDEX_SERVER} o/cache/index/indexserver_main.o o/raster/profiler.o
TARGET_CACHEINDEX_PKGLIBS := ${PKGLIBS_CORE}
TARGET_CACHEINDEX_LDFLAGS := ${LDFLAGS}

TARGET_CACHENODE_NAME := cache_node
TARGET_CACHENODE_OBJS = ${OBJ_COMMON} ${OBJ_NODE_SERVER} o/cache/node/nodeserver_main.o
TARGET_CACHENODE_PKGLIBS := ${PKGLIBS_CORE}
TARGET_CACHENODE_LDFLAGS := ${LDFLAGS} ${LDFLAGS_CL}

TARGET_CACHEEXPERIMENTS_NAME := cache_experiments
TARGET_CACHEEXPERIMENTS_OBJS = ${OBJ_COMMON} ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} o/cache/experiments/cache_experiments_main.o
TARGET_CACHEEXPERIMENTS_PKGLIBS := ${PKGLIBS_CORE}
TARGET_CACHEEXPERIMENTS_LDFLAGS := ${LDFLAGS} ${LDFLAGS_CL}

TARGET_CACHECLUSTEREXPERIMENTS_NAME := cluster_experiment
TARGET_CACHECLUSTEREXPERIMENTS_OBJS = ${OBJ_COMMON} ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} o/cache/experiments/cluster_experiment.o
TARGET_CACHECLUSTEREXPERIMENTS_PKGLIBS := ${PKGLIBS_CORE}
TARGET_CACHECLUSTEREXPERIMENTS_LDFLAGS := ${LDFLAGS} ${LDFLAGS_CL}

TARGET_LOCALSETUP_NAME := local_setup
TARGET_LOCALSETUP_OBJS = ${OBJ_COMMON} ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} o/cache/experiments/local_setup.o
TARGET_LOCALSETUP_PKGLIBS := ${PKGLIBS_CORE}
TARGET_LOCALSETUP_LDFLAGS := ${LDFLAGS} ${LDFLAGS_CL}

TARGET_TESTCLIENT_NAME := test_client
TARGET_TESTCLIENT_OBJS =  ${OBJ_COMMON} ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} o/cache/experiments/nbclient.o o/services/httpservice.o o/services/httpparsing.o o/services/ogcservice.o
TARGET_TESTCLIENT_PKGLIBS := ${PKGLIBS_CORE}
TARGET_TESTCLIENT_LDFLAGS := ${LDFLAGS} ${LDFLAGS_CL}


# rserver is broken atm, needs to go into a module
#${EXERSERVER}:	o/rserver/rserver.o o/operators/queryrectangle.o o/operators/queryprofiler.o ${OBJ_DATATYPES_NOCL} ${OBJ_UTIL} o/raster/profiler.o o/cache/common.o o/util/server_nonblocking.o
#	${CPP} $+ -o $@ ${LDFLAGS} ${LDFLAGS_R}
