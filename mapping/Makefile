#
# General configuration
#

LD_LIBRARY_PATH := /usr/local/lib/:$(LD_LIBRARY_PATH)
CPATH := /usr/local/include/:$(CPATH)

#CPP=g++
CPP=clang++-3.5
CPPFLAGS=-g -O2 -Wall -Wextra -pedantic-errors -std=c++11 -I. -I/usr/include/jsoncpp/ -I/usr/include/gdal
CPPFLAGS_R=-I/usr/local/lib/R/site-library/***REMOVED***/include/ -I/usr/local/lib/R/site-library/***REMOVED***/include/ -I/usr/lib/R/site-library/***REMOVED***/include/ -I/usr/share/R/include
LDFLAGS=-lgdal -ljsoncpp -lsqlite3 -lz -lbz2 -lpthread -pthread -lpng -lturbojpeg -lpqxx -lgeos -lcurl -lboost_date_time
LDFLAGS_CL=-lOpenCL
LDFLAGS_R=-L/usr/lib/R/lib -lR -L/usr/local/lib/R/site-library/***REMOVED***/lib -l***REMOVED*** -Wl,-rpath,/usr/local/lib/R/site-library/***REMOVED***/lib
LDFLAGS_CGI=-luriparser
LDFLAGS_GTEST=-lgtest -lgtest_main -lpthread
CL_SOURCES:=$(wildcard operators/*/*.cl)
CL_HEADERS:=$(CL_SOURCES:.cl=.cl.h)

VALGRIND=valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --suppressions=test/systemtests/valgrind_suppression.supp --error-exitcode=2
TEST_MODE=run

TEST_QUERIES:=$(wildcard test/systemtests/queries/*.json)
TEST_LOGS:=$(TEST_QUERIES:.json=.log)

#
# All object files, grouped by subsystem
#
OBJ_SERVICES=o/services/wfs_request.o
OBJ_RASTER=o/raster/colors.o o/raster/opencl.o o/raster/profiler.o
OBJ_RASTERDB=o/rasterdb/rasterdb.o o/rasterdb/backend.o o/rasterdb/backend_local.o o/rasterdb/backend_remote.o
OBJ_DATATYPES_COMMON=o/datatypes/attributes.o o/datatypes/spatiotemporal.o o/datatypes/raster/import_gdal.o o/datatypes/raster/export_pgm.o o/datatypes/raster/export_yuv.o o/datatypes/raster/export_png.o o/datatypes/raster/export_jpeg.o o/datatypes/simplefeaturecollection.o o/datatypes/pointcollection.o o/datatypes/linecollection.o o/datatypes/polygoncollection.o o/datatypes/simplefeaturecollections/geosgeomutil.o o/datatypes/simplefeaturecollections/wkbutil.o o/datatypes/unit.o
OBJ_DATATYPES=${OBJ_DATATYPES_COMMON} o/datatypes/raster/raster.o
OBJ_DATATYPES_NOCL=${OBJ_DATATYPES_COMMON} o/datatypes/raster/raster_nocl.o
OBJ_PLOT=o/datatypes/plots/histogram.o o/datatypes/plots/text.o o/datatypes/plots/png.o
OBJ_UTIL=o/util/gdal.o o/util/hash.o o/util/curl.o o/util/sqlite.o o/util/sunpos.o o/util/binarystream.o o/util/csvparser.o o/util/base64.o o/util/configuration.o o/util/debug.o o/util/timemodification.o o/util/log.o
OBJ_POINTVISUALIZATION=o/pointvisualization/BoundingBox.o o/pointvisualization/Circle.o o/pointvisualization/Coordinate.o o/pointvisualization/Dimension.o o/pointvisualization/FindResult.o o/pointvisualization/QuadTreeNode.o o/pointvisualization/CircleClusteringQuadTree.o
OBJ_CONVERTERS=o/converters/converter.o o/converters/raw.o
OBJ_OPERATORS_RASTER=o/operators/raster/matrixkernel.o o/operators/raster/opencl.o o/operators/raster/expression.o o/operators/raster/projections.o o/operators/raster/rastersource.o o/operators/raster/r.o o/operators/raster/classification.o
OBJ_OPERATORS_POINTS=o/operators/points/pgpointsource.o o/operators/points/gfbiopointsource.o o/operators/points/points_filter_by_range.o o/operators/points/points_difference.o o/operators/points/points_cluster.o o/operators/points/csvpointsource.o
OBJ_OPERATORS_GEOMETRY=o/operators/geometry/wktsource.o
OBJ_OPERATORS_COMBINED=o/operators/combined/points2raster.o o/operators/combined/raster_metadata_to_points.o o/operators/combined/points2cluster_raster.o o/operators/combined/filterpointsbygeometry.o
OBJ_OPERATORS_MSAT=o/operators/msat/temperature.o o/operators/msat/reflectance.o o/operators/msat/solarangle.o o/operators/msat/radiance.o o/operators/msat/pansharpening.o o/operators/msat/gccthermthresholddetection.o o/operators/msat/co2correction.o
OBJ_OPERATORS_PLOTS=o/operators/plots/histogram.o o/operators/plots/points2histogram.o o/operators/plots/points_metadata_tograph.o
OBJ_OPERATORS_TEMPORAL=o/operators/temporal/timeshiftoperator.o
OBJ_OPERATORS_GFBIO=o/operators/gfbio/pangaeasource.o
OBJ_OPERATORS_ALL=$(OBJ_OPERATORS_RASTER) $(OBJ_OPERATORS_POINTS) $(OBJ_OPERATORS_GEOMETRY) $(OBJ_OPERATORS_COMBINED) $(OBJ_OPERATORS_MSAT) $(OBJ_OPERATORS_PLOTS) $(OBJ_OPERATORS_GFBIO) ${OBJ_OPERATORS_TEMPORAL}

OBJ_OPERATORS_ALLSTUBS=${OBJ_OPERATORS_ALL:o/operators/%=o/operators_stub/%}

OBJ_OPERATORS_COMMON=o/operators/operator.o o/operators/queryrectangle.o o/operators/queryprofiler.o
OBJ_OPERATORS=${OBJ_OPERATORS_COMMON} ${OBJ_OPERATORS_ALL}
OBJ_OPERATORS_STUBS=${OBJ_OPERATORS_COMMON} ${OBJ_OPERATORS_ALLSTUBS}

OBJ_CACHE_COMMON=o/cache/common.o o/cache/priv/transfer.o o/cache/priv/connection.o o/cache/priv/redistribution.o o/cache/priv/cache_stats.o o/cache/priv/cache_structure.o o/cache/node/node_cache.o o/cache/manager.o o/cache/priv/caching_strategy.o o/util/nio.o
OBJ_COMMON=${OBJ_CACHE_COMMON} ${OBJ_RASTER} ${OBJ_RASTERDB} ${OBJ_DATATYPES} ${OBJ_CONVERTERS} ${OBJ_PLOT} ${OBJ_POINTVISUALIZATION} ${OBJ_OPERATORS} ${OBJ_UTIL} ${OBJ_SERVICES}

#
# All executables
#
EXE=mapping_manager
EXECGI=../htdocs/cgi-bin/mapping
EXERSERVER=r_server
EXEGTEST=gtest
EXEGEOMETRYTESTBED=geometrytestbed
EXETILESERVER=tileserver
EXECACHEINDEX=cache_index
EXECACHENODE=cache_node


.PHONY: clean systemtest system_testenvironment system_testcases unittest doc

.DELETE_ON_ERROR:

.SECONDEXPANSION:

all: $(CL_HEADERS) ${EXE} ${EXECGI} ${EXERSERVER} ${EXETILESERVER} ${EXECACHEINDEX} ${EXECACHENODE} ${EXEGTEST} ${EXEGEOMETRYTESTBED}
without-rserver: $(CL_HEADERS) ${EXE} ${EXECGI} ${EXETILESERVER} ${EXECACHEINDEX} ${EXECACHENODE} ${EXEGTEST} ${EXEGEOMETRYTESTBED}

#
# Dependencies and compiler flags of all executables
#

${EXE}:	o/mapping_manager.o ${OBJ_COMMON}
	${CPP} $+ -o $@ ${LDFLAGS} ${LDFLAGS_CL}

${EXECGI}:	o/cgi.o o/cache/client.o ${OBJ_COMMON}
	${CPP} $+ -o $@  ${LDFLAGS} ${LDFLAGS_CL} ${LDFLAGS_CGI}

${EXERSERVER}:	o/rserver/rserver.o o/operators/queryrectangle.o o/operators/queryprofiler.o ${OBJ_DATATYPES_NOCL} ${OBJ_UTIL} o/raster/colors.o o/raster/profiler.o
	${CPP} $+ -o $@ ${LDFLAGS} ${LDFLAGS_R}
	
${EXEGTEST}:	${OBJ_COMMON} o/cache/node/nodeserver.o o/cache/node/delivery.o o/cache/index/indexserver.o o/cache/index/querymanager.o o/cache/index/index_cache.o o/cache/index/reorg_strategy.o o/cache/client.o o/test/unittests/init.o o/test/unittests/example.o o/test/unittests/simplefeaturecollections/points.o o/test/unittests/simplefeaturecollections/lines.o o/test/unittests/simplefeaturecollections/polygons.o o/test/unittests/temporal/timeshift.o o/test/unittests/cache/util.o o/test/unittests/cache/stcache.o o/test/unittests/units.o #o/test/unittests/cache/distribution.o
	${CPP} $+ -o $@ ${LDFLAGS} ${LDFLAGS_CL} ${LDFLAGS_GTEST}

${EXEGEOMETRYTESTBED}:	o/geometrytestbed.o ${OBJ_COMMON}
	${CPP} $+ -o $@ ${LDFLAGS} ${LDFLAGS_CL} 

${EXETILESERVER}: o/rasterdb/tileserver.o o/rasterdb/backend.o o/rasterdb/backend_local.o o/datatypes/attributes.o o/util/binarystream.o o/util/sqlite.o o/util/configuration.o 
	${CPP} $+ -o $@  ${LDFLAGS}

${EXECACHEINDEX}:	o/cache/index/indexserver_main.o o/cache/index/indexserver.o o/cache/index/querymanager.o o/cache/index/index_cache.o o/cache/index/reorg_strategy.o ${OBJ_CACHE_COMMON} ${OBJ_OPERATORS_COMMON} ${OBJ_DATATYPES_NOCL} ${OBJ_UTIL} o/raster/colors.o o/raster/profiler.o 
	${CPP} $+ -o $@ ${LDFLAGS}

${EXECACHENODE}:	o/cache/node/nodeserver_main.o o/cache/node/nodeserver.o o/cache/node/delivery.o ${OBJ_COMMON}
	${CPP}  $+ -o $@ ${LDFLAGS} ${LDFLAGS_CL}



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

o/%.o: %.cpp | $${@D}/.f
	${CPP} ${CPPFLAGS} -MMD -MF ${@:.o=.d} -c $< -o $@

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
systemtest:
	${MAKE} system_testenvironment
	${MAKE} system_testcases
	php test/systemtests/parse_logs.php >test/systemtests/logs/tests_$$(date '+%Y-%m-%d_%H-%M').log
	rm -f ${TEST_LOGS}
	echo "Testcases done";
	
system_testcases: ${EXE} ${TEST_LOGS}
	echo "All logs written"

system_testenvironment: ${EXE}
	rm -f test/systemtests/data/world1.dat test/systemtests/data/world1.db
	#./${EXE} createsource 4326 test/systemtests/data/world1.tif  >test/systemtests/data/test_world1.json
	export MAPPING_RASTERDB_BACKEND=local MAPPING_RASTERDB_LOCAL_PATH=test/systemtests/data/ ; ./${EXE} import world1 test/systemtests/data/world1.tif 1 0 0 2000000000 RAW

test/systemtests/queries/%.log: test/systemtests/queries/%.json ${EXE} test/systemtests/data/world1.dat 
	echo -n >$@
	export MAPPING_RASTERDB_BACKEND=local MAPPING_RASTERDB_LOCAL_PATH=test/systemtests/data/ ; if EXELOG=$$(./${EXE} testsemantic $< 2>&1) ; then echo "semantic_passed: yes" >>$@ ; else echo "semantic_passed: no\n$$EXELOG" >>$@ ; fi
	export MAPPING_RASTERDB_BACKEND=local MAPPING_RASTERDB_LOCAL_PATH=test/systemtests/data/ ; if EXELOG=$$(./${EXE} testquery $< 2>&1) ; then echo "hash_passed: yes" >>$@ ; else echo "hash_passed: no\n$$EXELOG" >>$@ ; fi
	#MAPPING_RASTERDB_LOCAL_PATH=test/systemtests/data/ ; if VGLOG=$$(timeout 180 ${VALGRIND} ./${EXE} testquery $< 2>&1) ; then echo "valgrind_passed: yes" >>$@ ; else echo "valgrind_passed: no\n$$VGLOG" >>$@ ; fi

unittest:
	${MAKE} ${EXEGTEST}
	./${EXEGTEST}

webinstall:
	php htdocs/get_css.php > htdocs/compiled/compiled.release.css
	php htdocs/get_javascript.php > htdocs/compiled/compiled.release.js
	php htdocs/get_templates.php > htdocs/compiled/compiled.release.soy.js

doc:
	./../docs/makeDoc

clean:
	rm -f ${EXE} ${EXECGI} ${EXERSERVER} ${EXETILESERVER} ${EXECACHEINDEX} ${EXECACHENODE} ${EXEGTEST}
	rm -f $(CL_HEADERS)
	find o/ -type f -name '*.o' -delete
	find o/ -type f -name '*.d' -delete
