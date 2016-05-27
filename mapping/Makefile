#
# General configuration
# These variables are for system-specific settings and can be overwritten in Makefile.local
# TODO: maybe we should use autotools or cmake or something..
#

# this is the default path when installing libgtest-dev on ubuntu
GTEST_DIR=/usr/src/gtest/

#CPP=g++
CPP=clang++-3.5
LLVM_SYMBOLIZER=/usr/bin/llvm-symbolizer-3.5
ASAN_OPTIONS=
# if you need better stack traces, use something like this. This will slow down the tests by a factor of 5-10!
#ASAN_OPTIONS=fast_unwind_on_malloc=0:malloc_context_size=10
UBSAN_OPTIONS=print_stacktrace=1


USE_R=true
USE_OPENCL=true
USE_ABCD=true

DBG_FLAGS=-g
OPT_FLAGS=-O2

# to override any of these options, create Makefile.local and set them there
-include Makefile.local

# now let's see if all our libraries are present
EXTERNAL_LIBS := jsoncpp gdal libpng zlib libpqxx libcurl sqlite3 libarchive
ifeq (${USE_ABCD},true)
	EXTERNAL_LIBS+= xerces-c
endif

EXTERNAL_LIBS_EXIST := $(shell pkg-config --exists $(EXTERNAL_LIBS) >/dev/null 2>&1 ; echo $$?)
ifneq ($(EXTERNAL_LIBS_EXIST),0)
$(shell pkg-config --print-errors $(EXTERNAL_LIBS))
$(error Could not find required library, we need $(EXTERNAL_LIBS))
endif



# The testcases are run with full error checking. Currently, the following two tools are used:
# AddressSanitizer, see http://clang.llvm.org/docs/AddressSanitizer.html
# UndefinedBehaviorSanitizer, see http://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html
SANITIZE_FLAGS:=-fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer
# -fno-sanitize-recover=undefined would exit on undefined behaviour, but needs a more recent version of clang
# -fsanitize=integer for warnings on (defined) unsigned overflows

CPPFLAGS:=${DBG_FLAGS} ${OPT_FLAGS} -Wall -Wextra -pedantic-errors -std=c++11 -I. $(shell pkg-config --cflags $(EXTERNAL_LIBS))
CPPFLAGS_R:=-I/usr/local/lib/R/site-library/***REMOVED***/include/ -I/usr/local/lib/R/site-library/***REMOVED***/include/ -I/usr/lib/R/site-library/***REMOVED***/include/ -I/usr/share/R/include
LDFLAGS:=$(shell pkg-config --libs $(EXTERNAL_LIBS)) -lpthread -pthread -lbz2 -lturbojpeg -lgeos -lboost_date_time
LDFLAGS_CL=-lOpenCL
LDFLAGS_R=-L/usr/lib/R/lib -lR -L/usr/local/lib/R/site-library/***REMOVED***/lib -l***REMOVED*** -Wl,-rpath,/usr/local/lib/R/site-library/***REMOVED***/lib
LDFLAGS_CGI=-lfcgi++ -lfcgi
LDFLAGS_GTEST=-lpthread
CL_SOURCES:=$(wildcard operators/*/*.cl) $(wildcard operators/*/*/*.cl)
CL_HEADERS:=$(CL_SOURCES:.cl=.cl.h)

TEST_QUERIES:=$(wildcard test/systemtests/queries/*.json)
TEST_LOGS:=$(TEST_QUERIES:.json=.log)


ifneq (${USE_OPENCL},true)
	CPPFLAGS := ${CPPFLAGS} -DMAPPING_NO_OPENCL=1
	CPPFLAGS_R := ${CPPFLAGS_R} -DMAPPING_NO_OPENCL=1
	LDFLAGS_CL := 
endif

#
# All object files, grouped by subsystem
#
OBJ_SERVICES=o/services/httpservice.o o/services/httpparsing.o o/services/user.o o/services/ogcservice.o o/services/wms.o o/services/wcs.o o/services/wfs.o o/services/plot.o o/services/provenance.o o/services/artifact.o
OBJ_RASTER=o/raster/opencl.o o/raster/profiler.o
OBJ_RASTERDB=o/rasterdb/rasterdb.o o/rasterdb/backend.o o/rasterdb/backend_local.o o/rasterdb/backend_remote.o
OBJ_USERDB=o/userdb/userdb.o o/userdb/backend_sqlite.o
OBJ_DATATYPES_COMMON=o/datatypes/attributes.o o/datatypes/spatiotemporal.o o/datatypes/raster/import_gdal.o o/datatypes/raster/export_pgm.o o/datatypes/raster/export_yuv.o o/datatypes/raster/export_png.o o/datatypes/raster/export_jpeg.o o/datatypes/simplefeaturecollection.o o/datatypes/pointcollection.o o/datatypes/linecollection.o o/datatypes/polygoncollection.o o/datatypes/simplefeaturecollections/geosgeomutil.o o/datatypes/simplefeaturecollections/wkbutil.o o/datatypes/unit.o o/datatypes/colorizer.o
OBJ_DATATYPES=${OBJ_DATATYPES_COMMON} o/datatypes/raster/raster.o
OBJ_DATATYPES_NOCL=${OBJ_DATATYPES_COMMON} o/datatypes/raster/raster_nocl.o
OBJ_PLOT=o/datatypes/plots/histogram.o o/datatypes/plots/text.o o/datatypes/plots/png.o
OBJ_UTIL=o/util/gdal.o o/util/sha1.o o/util/curl.o o/util/sqlite.o o/util/sunpos.o o/util/binarystream.o o/util/csvparser.o o/util/base64.o o/util/configuration.o o/util/formula.o o/util/debug.o o/util/timemodification.o o/util/log.o o/util/timeparser.o o/util/sizeutil.o
OBJ_POINTVISUALIZATION=o/pointvisualization/BoundingBox.o o/pointvisualization/Circle.o o/pointvisualization/Coordinate.o o/pointvisualization/Dimension.o o/pointvisualization/FindResult.o o/pointvisualization/QuadTreeNode.o o/pointvisualization/CircleClusteringQuadTree.o
OBJ_CONVERTERS=o/converters/converter.o o/converters/raw.o
OBJ_OPERATORS_SOURCE = o/operators/source/csv_source.o o/operators/source/gfbio_source.o o/operators/source/pangaea_source.o o/operators/source/postgres_source.o o/operators/source/rasterdb_source.o o/operators/source/wkt_source.o
OBJ_OPERATORS_PROCESSING_RASTER=o/operators/processing/raster/matrixkernel.o o/operators/processing/raster/expression.o o/operators/processing/raster/classification.o
OBJ_OPERATORS_PROCESSING_FEATURES=o/operators/processing/features/cluster.o o/operators/processing/features/difference.o o/operators/processing/features/numeric_attribute_filter.o o/operators/processing/features/point_in_polygon_filter.o
OBJ_OPERATORS_PROCESSING_COMBINED=o/operators/processing/combined/projection.o o/operators/processing/combined/r_script.o o/operators/processing/combined/raster_value_extraction.o o/operators/processing/combined/rasterization.o o/operators/processing/combined/timeshift.o
OBJ_OPERATORS_PROCESSING_METEOSAT=o/operators/processing/meteosat/temperature.o o/operators/processing/meteosat/reflectance.o o/operators/processing/meteosat/solarangle.o o/operators/processing/meteosat/radiance.o o/operators/processing/meteosat/pansharpening.o o/operators/processing/meteosat/gccthermthresholddetection.o o/operators/processing/meteosat/co2correction.o
OBJ_OPERATORS_PLOTS=o/operators/plots/histogram.o o/operators/plots/feature_attributes_plot.o
ifeq (${USE_ABCD},true)
	OBJ_OPERATORS_SOURCE += o/operators/source/abcd_source.o
endif
OBJ_OPERATORS_ALL=$(OBJ_OPERATORS_SOURCE) $(OBJ_OPERATORS_PROCESSING_RASTER) $(OBJ_OPERATORS_PROCESSING_FEATURES) $(OBJ_OPERATORS_PROCESSING_COMBINED) $(OBJ_OPERATORS_PROCESSING_METEOSAT) $(OBJ_OPERATORS_PLOTS)

OBJ_OPERATORS_ALLSTUBS=${OBJ_OPERATORS_ALL:o/operators/%=o/operators_stub/%}

OBJ_OPERATORS_COMMON=o/operators/operator.o o/operators/provenance.o o/operators/queryrectangle.o o/operators/queryprofiler.o
OBJ_OPERATORS=${OBJ_OPERATORS_COMMON} ${OBJ_OPERATORS_ALL}
OBJ_OPERATORS_STUBS=${OBJ_OPERATORS_COMMON} ${OBJ_OPERATORS_ALLSTUBS}

OBJ_CACHE_COMMON=o/cache/common.o o/cache/priv/shared.o o/cache/priv/requests.o o/cache/priv/connection.o o/cache/priv/redistribution.o o/cache/priv/cache_stats.o o/cache/priv/cache_structure.o o/cache/node/node_cache.o o/cache/manager.o o/cache/priv/caching_strategy.o o/cache/priv/cube.o
OBJ_COMMON=${OBJ_CACHE_COMMON} ${OBJ_RASTER} ${OBJ_RASTERDB} ${OBJ_DATATYPES} ${OBJ_CONVERTERS} ${OBJ_PLOT} ${OBJ_POINTVISUALIZATION} ${OBJ_OPERATORS} ${OBJ_UTIL}

SRC_UNITTEST:=$(wildcard test/unittests/*.cpp) $(wildcard test/unittests/*/*.cpp)
OBJ_UNITTEST:=$(patsubst test/%,o/test/%,$(SRC_UNITTEST:.cpp=.o))

#
# All executables
#
EXE=mapping_manager
EXE_SANITIZED=mapping_manager.san
EXECGI=../htdocs/cgi-bin/mapping
EXERSERVER=r_server
EXEGTEST=gtest
EXEPARSETESTLOGS=test/systemtests/parse_logs
EXETILESERVER=tileserver
EXECACHEINDEX=cache_index
EXECACHENODE=cache_node
EXECACHEEXP=cache_experiments
EXECLUSEXP=cluster_experiment


ifeq (${USE_R},true)
	TARGET_EXERSERVER:=${EXERSERVER}
else
	TARGET_EXERSERVER:=
endif


.PHONY: clean systemtest systemtest_testenvironment systemtest_testcases unittest doc

.DELETE_ON_ERROR:

.SECONDEXPANSION:

all: $(CL_HEADERS) ${EXE} ${EXECGI} ${TARGET_EXERSERVER} ${EXETILESERVER} ${EXECACHEINDEX} ${EXECACHENODE} ${EXEGTEST} ${EXEGEOMETRYTESTBED} ${EXECACHEEXP} ${EXECLUSEXP}

#
# Dependencies and compiler flags of all executables
#

${EXE}:	o/mapping_manager.o ${OBJ_COMMON} ${OBJ_USERDB}
	${CPP} $+ -o $@ ${LDFLAGS} ${LDFLAGS_CL}

${EXE_SANITIZED}:	$(patsubst %.o,%.san.o,o/mapping_manager.o ${OBJ_COMMON} ${OBJ_USERDB})
	${CPP} $+ -o $@ $(SANITIZE_FLAGS) ${LDFLAGS} ${LDFLAGS_CL}

${EXECGI}:	o/cgi.o ${OBJ_COMMON} ${OBJ_SERVICES} ${OBJ_USERDB}
	${CPP} $+ -o $@  ${LDFLAGS} ${LDFLAGS_CL} ${LDFLAGS_CGI}

${EXERSERVER}:	o/rserver/rserver.o o/operators/queryrectangle.o o/operators/queryprofiler.o ${OBJ_DATATYPES_NOCL} ${OBJ_UTIL} o/raster/profiler.o o/cache/common.o o/util/server_nonblocking.o
	${CPP} $+ -o $@ ${LDFLAGS} ${LDFLAGS_R}

${EXEGTEST}:	${OBJ_COMMON} ${OBJ_SERVICES} ${OBJ_USERDB} o/cache/index/node.o o/cache/index/index_cache_manager.o o/cache/node/nodeserver.o o/cache/node/delivery.o o/cache/node/node_manager.o o/cache/node/puzzle_util.o o/cache/index/indexserver.o o/cache/index/querymanager.o o/cache/index/index_cache.o o/cache/index/reorg_strategy.o o/cache/experiments/exp_util.o o/util/server_nonblocking.o ${OBJ_UNITTEST} o/libgtest.a
	${CPP} $+ -o $@ ${LDFLAGS} ${LDFLAGS_CL} ${LDFLAGS_GTEST}

${EXEPARSETESTLOGS}:	o/test/systemtests/parse_logs.o
	${CPP} $+ -o $@

${EXETILESERVER}: o/rasterdb/tileserver.o o/util/server_nonblocking.o o/rasterdb/backend.o o/rasterdb/backend_local.o o/datatypes/attributes.o o/datatypes/unit.o o/util/binarystream.o o/util/sha1.o o/util/sqlite.o o/util/configuration.o o/util/sizeutil.o o/util/log.o
	${CPP} $+ -o $@  ${LDFLAGS}

${EXECACHEINDEX}:	${OBJ_CACHE_COMMON} ${OBJ_OPERATORS_COMMON} ${OBJ_DATATYPES_NOCL} ${OBJ_UTIL} o/cache/index/node.o o/cache/index/index_cache_manager.o o/cache/index/indexserver_main.o o/cache/index/indexserver.o o/cache/index/querymanager.o o/cache/index/index_cache.o o/cache/index/reorg_strategy.o o/raster/profiler.o 
	${CPP} $+ -o $@ ${LDFLAGS}

${EXECACHENODE}:	${OBJ_COMMON} o/cache/node/nodeserver_main.o o/cache/node/nodeserver.o o/cache/node/delivery.o o/cache/node/node_manager.o o/cache/node/puzzle_util.o
	${CPP} $+ -o $@ ${LDFLAGS} ${LDFLAGS_CL}

${EXECACHEEXP}:	${OBJ_COMMON} o/cache/index/node.o o/cache/index/index_cache_manager.o o/cache/node/nodeserver.o o/cache/node/delivery.o o/cache/node/node_manager.o o/cache/node/puzzle_util.o o/cache/index/indexserver.o o/cache/index/querymanager.o o/cache/index/index_cache.o o/cache/index/reorg_strategy.o o/cache/experiments/exp_util.o o/cache/experiments/cache_experiments.o o/cache/experiments/cache_experiments_main.o
	${CPP} $+ -o $@ ${LDFLAGS} ${LDFLAGS_CL} ${LDFLAGS_GTEST}

${EXECLUSEXP} : ${OBJ_COMMON} o/cache/index/node.o o/cache/index/index_cache_manager.o o/cache/node/nodeserver.o o/cache/node/delivery.o o/cache/node/node_manager.o o/cache/node/puzzle_util.o o/cache/index/indexserver.o o/cache/index/querymanager.o o/cache/index/index_cache.o o/cache/index/reorg_strategy.o o/cache/experiments/exp_util.o o/cache/experiments/cache_experiments.o o/cache/experiments/cluster_experiment.o 
	${CPP} $+ -o $@ ${LDFLAGS} ${LDFLAGS_CL} ${LDFLAGS_GTEST}
#
# gtest library
# instructions taken from https://github.com/google/googletest/blob/master/googletest/README.md
#
o/gtest-all.o: ${GTEST_DIR}/src/gtest-all.cc
	${CPP} ${CPPFLAGS} -I${GTEST_DIR} -pthread -o $@ -c ${GTEST_DIR}/src/gtest-all.cc

o/libgtest.a: o/gtest-all.o
	ar -rv o/libgtest.a o/gtest-all.o



#
# static rules
#

# include the generated dependencies
-include o/*.d o/*/*.d o/*/*/*.d o/*/*/*/*.d

operators/%.cl.h: operators/%.cl
	xxd -i $+ | sed 's/unsigned /static const /g' > $@
	echo $+ | sed -e 's/\.cl$$//' -e 's:/:_:g' -e 's/\(^.*$$\)/static const std::string \1(\1_cl, \1_cl_len);/' >> $@

.PRECIOUS: operators/%.cl.h

o/rserver/%.o: rserver/%.cpp | $${@D}/.f
	${CPP} ${CPPFLAGS} ${CPPFLAGS_R} -MMD -MF ${@:.o=.d} -c $< -o $@

o/operators_stub/%.o: operators/%.cpp | $${@D}/.f
	${CPP} ${CPPFLAGS} -DMAPPING_OPERATOR_STUBS=1 -MMD -MF ${@:.o=.d} -c $< -o $@

o/%.o: %.cpp | $${@D}/.f $(CL_HEADERS)
	${CPP} ${CPPFLAGS} -MMD -MF ${@:.o=.d} -c $< -o $@

# .san.o = compiled with Sanitation
o/%.san.o: %.cpp | $${@D}/.f $(CL_HEADERS)
	${CPP} ${CPPFLAGS} ${SANITIZE_FLAGS} -MMD -MF ${@:.o=.d} -c $< -o $@

o/%/.f:
	mkdir -p $(dir $@)
	touch $@
	
o/.f:
	touch $@

.PRECIOUS: o/%/.f

.PRECIOUS: o/.f



#
# Targets for data import
#
btw2015_paper_demo_datasource:	${EXE}
	/mnt/data/raster_import/btw2015_paper_demo.sh $$(readlink -f ./${EXE})

cruts_datasource:	${EXE}
	/mnt/data/raster_import/cruts.sh $$(readlink -f ./${EXE})

glc2000_global_datasource:	${EXE}
	/mnt/data/raster_import/glc2000_global.sh $$(readlink -f ./${EXE})

isric_wise_datasource:	${EXE}
	/mnt/data/raster_import/isric_wise.sh $$(readlink -f ./${EXE})

modis_npp_datasource:
	/mnt/data/raster_import/modis_npp.sh $$(readlink -f ./${EXE})

modis_vcf_datasource:
	/mnt/data/raster_import/modis_vcf.sh $$(readlink -f ./${EXE})

msat_datasource:	${EXE}
	/mnt/data/raster_import/msat.sh $$(readlink -f ./${EXE})

srtm_datasource:	${EXE}
	/mnt/data/raster_import/srtm.sh $$(readlink -f ./${EXE})

worldclim_datasource:	${EXE}
	/mnt/data/raster_import/worldclim.sh $$(readlink -f ./${EXE})


#
# All the remaining phony targets
#
systemtest: ${EXEPARSETESTLOGS}
	${MAKE} systemtest_testenvironment
	${MAKE} systemtest_testcases
	${EXEPARSETESTLOGS} >test/systemtest_latest.xml
	rm -f ${TEST_LOGS}
	echo "Testcases done";

systemtest_testcases: ${EXE} ${TEST_LOGS}
	echo "All logs written"

systemtest_testenvironment: ${EXE}
	rm -f test/systemtests/data/world1.dat test/systemtests/data/world1.db
	rm -f test/systemtests/data/ndvi.dat test/systemtests/data/ndvi.db
	#./${EXE} createsource 4326 test/systemtests/data/world1.tif  >test/systemtests/data/test_world1.json
	export MAPPING_CONFIGURATION=test/systemtests/mapping_test.conf ; ./${EXE} import world1 test/systemtests/data/world1.tif 1 0 0 2000000000 RAW
	export MAPPING_CONFIGURATION=test/systemtests/mapping_test.conf ; bash test/systemtests/data/ndvi/import.sh 

# note: use /usr/bin/time instead of time, because the latter may be a shell's builtin without the required POSIX options
# The || true at the end is to prevent make from aborting after the first failed test. The actual return code is in the log. 
test/systemtests/queries/%.log: test/systemtests/queries/%.json ${EXE} ${EXE_SANITIZED}
	export MAPPING_CONFIGURATION=test/systemtests/mapping_test.conf ASAN_SYMBOLIZER_PATH="${LLVM_SYMBOLIZER}" ASAN_OPTIONS="${ASAN_OPTIONS}" UBSAN_OPTIONS="${UBSAN_OPTIONS}" LSAN_OPTIONS="suppressions=test/systemtests/lsan.suppressions" ; /usr/bin/time --format="\nTESTCASE_ELAPSED_TIME: %e\nTESTCASE_RETURN_CODE: %x" ./${EXE_SANITIZED} testquery $< >$@ 2>&1 || true

unittest: ${EXEGTEST}
	./${EXEGTEST} --gtest_color=yes --gtest_output="xml:test/unittest_latest.xml" || true

webinstall:
	php htdocs/get_css.php > htdocs/compiled/compiled.release.css
	php htdocs/get_javascript.php > htdocs/compiled/compiled.release.js
	php htdocs/get_templates.php > htdocs/compiled/compiled.release.soy.js

doc:
	./../docs/makeDoc

clean:
	rm -f ${EXE} ${EXE_SANITIZED} ${EXECGI} ${EXERSERVER} ${EXETILESERVER} ${EXECACHEINDEX} ${EXECACHENODE} ${EXEGTEST} ${EXEPARSETESTLOGS} ${EXECACHEEXP} ${EXECLUSEXP}
	rm -f $(CL_HEADERS)
	find o/ -type f -name '*.o' -delete
	find o/ -type f -name '*.d' -delete
	find o/ -type f -name '*.a' -delete
