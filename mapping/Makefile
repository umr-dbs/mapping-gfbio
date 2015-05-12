# GCC 4.8.1, -O2
# real    0m27.775s
# clang
# real    0m22.968s

LD_LIBRARY_PATH := /usr/local/lib/:$(LD_LIBRARY_PATH)
CPATH := /usr/local/include/:$(CPATH)

#CPP=g++
CPP=clang++-3.5
CPPFLAGS=-g -O0 -Wall -Wextra -pedantic-errors -std=c++11 -I. -I/usr/include/jsoncpp/ #-DMAPPING_NO_OPENCL
CPPFLAGS_R=-I/usr/local/lib/R/site-library/***REMOVED***/include/ -I/usr/local/lib/R/site-library/***REMOVED***/include/ -I/usr/share/R/include
LDFLAGS=-lgdal -ljsoncpp -lsqlite3 -lz -lbz2 -lpthread -pthread -lOpenCL -lpng -lturbojpeg -lpqxx -lgeos -lcurl -lboost_date_time
LDFLAGS_R=-L/usr/lib/R/lib -lR -L/usr/local/lib/R/site-library/***REMOVED***/lib -l***REMOVED*** -Wl,-rpath,/usr/local/lib/R/site-library/***REMOVED***/lib
LDFLAGS_CGI=-luriparser
LDFLAGS_GTEST=-lgtest -lgtest_main -lpthread
CL_SOURCES:=$(wildcard operators/*/*.cl)
CL_HEADERS:=$(CL_SOURCES:.cl=.cl.h)
OBJ_SERVICES=o/services_wfs_request.o
OBJ_RASTER=o/rasterdb_rasterdb.o o/rasterdb_backend.o o/rasterdb_backend_local.o o/rasterdb_backend_remote.o o/raster_colors.o o/raster_opencl.o o/raster_profiler.o
OBJ_DATATYPES=o/datatypes_attributes.o o/datatypes_spatiotemporal.o o/datatypes_raster_raster.o o/datatypes_raster_import_gdal.o o/datatypes_raster_export_pgm.o o/datatypes_raster_export_yuv.o o/datatypes_raster_export_png.o o/datatypes_raster_export_jpeg.o o/datatypes_simplefeaturecollection.o o/datatypes_pointcollection.o o/datatypes_linecollection.o o/datatypes_polygoncollection.o o/datatypes_simplefeaturecollections_geosgeomutil.o o/datatypes_simplefeaturecollections_wkbutil.o
OBJ_PLOT=o/datatypes_plots_histogram.o o/datatypes_plots_text.o o/datatypes_plots_png.o
OBJ_UTIL=o/util_gdal.o o/util_hash.o o/util_curl.o o/util_sqlite.o o/util_sunpos.o o/util_binarystream.o o/util_csvparser.o o/util_base64.o o/util_configuration.o o/util_debug.o
OBJ_POINTVISUALIZATION=o/pointvisualization_BoundingBox.o o/pointvisualization_Circle.o o/pointvisualization_Coordinate.o o/pointvisualization_Dimension.o o/pointvisualization_FindResult.o o/pointvisualization_QuadTreeNode.o o/pointvisualization_CircleClusteringQuadTree.o
OBJ_CONVERTERS=o/converters_converter.o o/converters_raw.o
OBJ_OPERATORS_RASTER=o/operators_raster_matrixkernel.o o/operators_raster_opencl.o o/operators_raster_expression.o o/operators_raster_projections.o o/operators_raster_rastersource.o o/operators_raster_r.o o/operators_raster_classification.o
OBJ_OPERATORS_POINTS=o/operators_points_pgpointsource.o o/operators_points_gfbiopointsource.o o/operators_points_points_filter_by_range.o o/operators_points_points_difference.o o/operators_points_points_cluster.o o/operators_points_csvpointsource.o o/operators_points_isolineextractor.o
OBJ_OPERATORS_GEOMETRY=o/operators_geometry_wktsource.o#o/operators_geometry_testgeometrysource.o o/operators_geometry_osmgeometrysource.o
OBJ_OPERATORS_COMBINED=o/operators_combined_points2raster.o o/operators_combined_raster_metadata_to_points.o o/operators_combined_points2cluster_raster.o o/operators_combined_filterpointsbygeometry.o
OBJ_OPERATORS_MSAT=o/operators_msat_temperature.o o/operators_msat_reflectance.o o/operators_msat_solarangle.o o/operators_msat_radiance.o o/operators_msat_pansharpening.o o/operators_msat_gccthermthresholddetection.o
OBJ_OPERATORS_PLOTS=o/operators_plots_histogram.o o/operators_plots_points2histogram.o o/operators_plots_points_metadata_tograph.o
OBJ_OPERATORS_TEMPORAL=o/operators_temporal_timeshiftoperator.o
OBJ_OPERATORS_GFBIO=o/operators_gfbio_pangaeasource.o
OBJ_OPERATORS=o/operators_operator.o $(OBJ_OPERATORS_RASTER) $(OBJ_OPERATORS_POINTS) $(OBJ_OPERATORS_GEOMETRY) $(OBJ_OPERATORS_COMBINED) $(OBJ_OPERATORS_MSAT) $(OBJ_OPERATORS_PLOTS) ${OBJ_OPERATORS_TEMPORAL} $(OBJ_OPERATORS_GFBIO)
OBJ_COMMON=${OBJ_RASTER} ${OBJ_DATATYPES} ${OBJ_CONVERTERS} ${OBJ_PLOT} ${OBJ_POINTVISUALIZATION} ${OBJ_OPERATORS} ${OBJ_UTIL} ${OBJ_SERVICES}
OBJ_EXE=o/main_rasterdb.o ${OBJ_COMMON}
OBJ_EXECGI=o/main_cgi.o ${OBJ_COMMON}
OBJ_EXEGTEST=${OBJ_COMMON} o/gtest_init.o o/gtest_example.o o/gtest_simplefeaturecollections_points.o o/gtest_simplefeaturecollections_lines.o o/gtest_simplefeaturecollections_polygons.o
OBJ_EXEGEOMETRYTESTBED=o/main_geometrytestbed.o ${OBJ_COMMON}
# TODO: more fine grained selection of the required objects
OBJ_RSERVER=o/rserver_rserver.o o/operators_operator.o ${OBJ_CONVERTERS} ${OBJ_RASTER} ${OBJ_DATATYPES} ${OBJ_UTIL}
EXE=mapping_manager
EXECGI=../htdocs/cgi-bin/mapping
EXERSERVER=r_server
EXEGTEST=gtest
EXEGEOMETRYTESTBED=geometrytestbed
EXETILESERVER=tileserver


VALGRIND=valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --suppressions=test/systemtests/valgrind_suppression.supp --error-exitcode=2
TEST_MODE=run

TEST_QUERIES:=$(wildcard test/systemtests/queries/*.json)
TEST_LOGS:=$(TEST_QUERIES:.json=.log)


.SECONDARY: $(CL_HEADERS)

.PHONY: clean systemtest system_testenvironment system_testcases unittest doc

.DELETE_ON_ERROR:

operators/%.cl.h: operators/%.cl
	xxd -i $+ | sed 's/unsigned /static const /g' > $@
	echo $+ | sed -e 's/\.cl$$//' -e 's:/:_:g' -e 's/\(^.*$$\)/static const std::string \1(\1_cl, \1_cl_len);/' >> $@ 

o/util_%.o: util/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/pointvisualization_%.o: pointvisualization/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/services_%.o: services/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/raster_%.o: raster/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/rasterdb_%.o: rasterdb/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/converters_%.o: converters/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/datatypes_%.o: datatypes/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@
	
o/datatypes_raster_%.o: datatypes/raster/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/datatypes_plots_%.o: datatypes/plots/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@
	
o/datatypes_simplefeaturecollections_%.o: datatypes/simplefeaturecollections/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/rserver_%.o: rserver/%.cpp
	${CPP} ${CPPFLAGS} ${CPPFLAGS_R} -c $+ -o $@

o/operators_%.o: operators/%.cpp $(CL_HEADERS)
	${CPP} ${CPPFLAGS} -c $< -o $@

o/operators_raster_%.o: operators/raster/%.cpp $(CL_HEADERS)
	${CPP} ${CPPFLAGS} -c $< -o $@

o/operators_msat_%.o: operators/msat/%.cpp $(CL_HEADERS)
	${CPP} ${CPPFLAGS} -c $< -o $@

o/operators_points_%.o: operators/points/%.cpp $(CL_HEADERS)
	${CPP} ${CPPFLAGS} -c $< -o $@

o/operators_geometry_%.o: operators/geometry/%.cpp $(CL_HEADERS)
	${CPP} ${CPPFLAGS} -c $< -o $@

o/operators_combined_%.o: operators/combined/%.cpp $(CL_HEADERS)
	${CPP} ${CPPFLAGS} -c $< -o $@

o/operators_plots_%.o: operators/plots/%.cpp $(CL_HEADERS)
	${CPP} ${CPPFLAGS} -c $< -o $@
	
o/operators_temporal_%.o: operators/temporal/%.cpp $(CL_HEADERS)
	${CPP} ${CPPFLAGS} -c $< -o $@
	
o/operators_gfbio_%.o: operators/gfbio/%.cpp $(CL_HEADERS)
	${CPP} ${CPPFLAGS} -c $< -o $@

o/gtest_%.o: test/unittests/%.cpp $(CL_HEADERS)
	${CPP} ${CPPFLAGS} -c $< -o $@

o/gtest_simplefeaturecollections_%.o: test/unittests/simplefeaturecollections/%.cpp $(CL_HEADERS)
	${CPP} ${CPPFLAGS} -c $< -o $@

o/main_%.o: %.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@


all: ${EXE} ${EXECGI} ${EXERSERVER} ${EXETILESERVER}

${EXE}:	${OBJ_EXE}
	${CPP} $+ ${LDFLAGS} -o $@

${EXECGI}:	${OBJ_EXECGI}
	${CPP} $+ ${LDFLAGS} ${LDFLAGS_CGI} -o $@

${EXERSERVER}:	${OBJ_RSERVER}
	${CPP} ${LDFLAGS} ${LDFLAGS_R} $+ -o $@
	
${EXEGTEST}:	${OBJ_EXEGTEST}
	${CPP} $+ ${LDFLAGS} ${LDFLAGS_GTEST} -o $@

${EXEGEOMETRYTESTBED}:	${OBJ_EXEGEOMETRYTESTBED}
	${CPP} $+ ${LDFLAGS} -o $@

${EXETILESERVER}: o/rasterdb_tileserver.o o/rasterdb_backend.o o/rasterdb_backend_local.o o/datatypes_attributes.o o/util_binarystream.o o/util_sqlite.o o/util_configuration.o
	${CPP} ${LDFLAGS} $+ -o $@


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
	rm -f o/*.o ${EXE} ${EXECGI} ${EXERSERVER} $(CL_HEADERS)
