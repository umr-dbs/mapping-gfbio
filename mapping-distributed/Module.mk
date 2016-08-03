
#
# Support for distributed data storage, caching and processing
#


#
# Helper variables
#
OBJ_NODE_SERVER = o/core/cache/node/node_config.o o/core/cache/node/nodeserver.o o/core/cache/node/delivery.o o/core/cache/node/node_manager.o o/core/cache/node/manager/local_manager.o o/core/cache/node/manager/local_replacement.o o/core/cache/node/manager/remote_manager.o o/core/cache/node/manager/hybrid_manager.o o/core/cache/node/puzzle_util.o
OBJ_INDEX_SERVER = o/core/cache/index/index_config.o o/core/cache/index/node.o o/core/cache/index/index_cache_manager.o o/core/cache/index/indexserver.o o/core/cache/index/querymanager.o o/core/cache/index/query_manager/default_query_manager.o o/core/cache/index/query_manager/simple_query_manager.o o/core/cache/index/query_manager/emkde_query_manager.o o/core/cache/index/query_manager/late_query_manager.o o/core/cache/index/index_cache.o o/core/cache/index/reorg_strategy.o
OBJ_CACHE_EXP = o/core/cache/experiments/exp_util.o o/core/cache/experiments/cache_experiments.o
OBJ_CACHE_EXP += o/core/operators/processing/combined/projection.o o/core/operators/processing/combined/timeshift.o


OBJS_CORE += o/mapping-distributed/rasterdb/backend_remote.o


#
# GTest
#
SRC_GTEST:=$(wildcard ${MODULEPATH}test/unittests/*.cpp) $(wildcard ${MODULEPATH}test/unittests/*/*.cpp)
OBJS_GTEST += $(patsubst ${MODULEPATH}test/%,o/mapping-distributed/test/%,$(SRC_GTEST:.cpp=.o))
# TODO: these dependencies are ridiculous and need to be cleaned up
OBJS_GTEST += ${OBJ_INDEX_SERVER} ${OBJ_NODE_SERVER} ${OBJ_CACHE_EXP} 



ALL_TARGETS += TILESERVER CACHEINDEX CACHENODE CACHEEXPERIMENTS CACHECLUSTEREXPERIMENTS LOCALSETUP TESTCLIENT

TARGET_TILESERVER_NAME := tileserver
TARGET_TILESERVER_OBJS = o/mapping-distributed/rasterdb/tileserver.o o/core/util/server_nonblocking.o o/core/rasterdb/backend.o o/core/rasterdb/backend_local.o o/core/datatypes/attributes.o o/core/datatypes/unit.o o/core/util/binarystream.o o/core/util/sha1.o o/core/util/sqlite.o o/core/util/configuration.o o/core/util/sizeutil.o o/core/util/log.o
TARGET_TILESERVER_PKGLIBS = ${PKGLIBS_CORE}
TARGET_TILESERVER_LDFLAGS = ${LDFLAGS}

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
