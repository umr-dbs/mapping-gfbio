# GCC 4.8.1, -O2
# real    0m27.775s
# clang
# real    0m22.968s

LD_LIBRARY_PATH := /usr/local/lib/:$(LD_LIBRARY_PATH)
CPATH := /usr/local/include/:$(CPATH)

#CPP=g++
CPP=clang++
CPPFLAGS=-g -O2 -Wall -Wextra -pedantic-errors -std=c++11 -I. -I/usr/include/jsoncpp/
LDFLAGS=-lgdal -ljsoncpp -lsqlite3 -lz -lbz2 -lpthread -pthread -lOpenCL -lpng -lturbojpeg -lpqxx -lgeos -lcurl
LDFLAGS_CGI=-luriparser
CL_SOURCES=$(wildcard operators/*/*.cl)
CL_HEADERS=$(CL_SOURCES:.cl=.cl.h)
OBJ_RASTER=o/raster_raster.o o/raster_rastersource.o o/raster_colors.o o/raster_opencl.o o/raster_profiler.o o/raster_metadata.o o/raster_import_gdal.o o/raster_export_pgm.o o/raster_export_yuv.o o/raster_export_png.o o/raster_export_jpeg.o o/raster_pointcollection.o o/raster_histogram.o
OBJ_UTIL=o/util_hash.o o/util_curl.o o/util_sqlite.o
OBJ_CONVERTERS=o/converters_converter.o o/converters_raw.o
OBJ_OPERATORS_RASTER=o/operators_raster_matrixkernel.o o/operators_raster_opencl.o o/operators_raster_expression.o o/operators_raster_projections.o o/operators_raster_rastersource.o o/operators_raster_playground.o o/operators_raster_histogram.o
OBJ_OPERATORS_POINTS=o/operators_points_pgpointsource.o o/operators_points_gfbiopointsource.o
OBJ_OPERATORS_GEOMETRY=
OBJ_OPERATORS_COMBINED=o/operators_combined_points2raster.o o/operators_combined_filterpointsbyraster.o o/operators_combined_pointRasterHistogram.o
OBJ_OPERATORS_MSAT=o/operators_msat_radiance.o o/operators_msat_temperature.o
OBJ_OPERATORS=o/operators_operator.o $(OBJ_OPERATORS_RASTER) $(OBJ_OPERATORS_POINTS) $(OBJ_OPERATORS_GEOMETRY) $(OBJ_OPERATORS_COMBINED) $(OBJ_OPERATORS_MSAT)
OBJ_COMMON=${OBJ_RASTER} ${OBJ_CONVERTERS} ${OBJ_OPERATORS}  ${OBJ_UTIL}
OBJ_EXE=o/main_rasterdb.o ${OBJ_COMMON}
OBJ_EXECGI=o/main_cgi.o ${OBJ_COMMON}
EXE=mapping_manager
EXECGI=../htdocs/cgi-bin/mapping


DATASET_C0=data/xrit2/H-000-MSG2__-MSG2________-VIS006___-000001___-201006061200-C_
DATASET_C1=data/xrit2/H-000-MSG2__-MSG2________-VIS008___-000001___-201006061200-C_
DATASET_C2=data/xrit2/H-000-MSG2__-MSG2________-IR_016___-000001___-201006061200-C_
DATASET_C3=data/xrit2/H-000-MSG2__-MSG2________-IR_039___-000001___-201006061200-C_
DATASET_C4=data/xrit2/H-000-MSG2__-MSG2________-WV_062___-000001___-201006061200-C_
DATASET_C5=data/xrit2/H-000-MSG2__-MSG2________-WV_073___-000001___-201006061200-C_
DATASET_C6=data/xrit2/H-000-MSG2__-MSG2________-IR_087___-000001___-201006061200-C_
DATASET_C7=data/xrit2/H-000-MSG2__-MSG2________-IR_097___-000001___-201006061200-C_
DATASET_C8=data/xrit2/H-000-MSG2__-MSG2________-IR_108___-000001___-201006061200-C_
DATASET_C9=data/xrit2/H-000-MSG2__-MSG2________-IR_120___-000001___-201006061200-C_
DATASET_C10=data/xrit2/H-000-MSG2__-MSG2________-IR_134___-000001___-201006061200-C_
DATASET_TMEAN=data/tmean_1_10.tif

.SECONDARY: $(CL_HEADERS)

.DELETE_ON_ERROR:

operators/%.cl.h: operators/%.cl
	xxd -i $+ | sed 's/unsigned /static const /g' > $@
	echo $+ | sed -e 's/\.cl$$//' -e 's:/:_:g' -e 's/\(^.*$$\)/static const std::string \1(\1_cl, \1_cl_len);/' >> $@ 

o/util_%.o: util/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/raster_%.o: raster/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/converters_%.o: converters/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

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


o/main_%.o: %.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@


all: ${EXE} ${EXECGI}

${EXE}:	${OBJ_EXE}
	${CPP} $+ ${LDFLAGS} -o $@

${EXECGI}:	${OBJ_EXECGI}
	${CPP} $+ ${LDFLAGS} ${LDFLAGS_CGI} -o $@


msat2datasource:	${EXE}
	rm -f datasources/msat2.dat datasources/msat2.db
	#./${EXE} createsource 62866 ${DATASET_C0} ${DATASET_C1} ${DATASET_C2} ${DATASET_C3} ${DATASET_C4} ${DATASET_C5} ${DATASET_C6} ${DATASET_C7} ${DATASET_C8} ${DATASET_C9} ${DATASET_C10} >datasources/msat2.json
	./${EXE} import datasources/msat2.json ${DATASET_C0} 1 0 42 RAW
	./${EXE} import datasources/msat2.json ${DATASET_C1} 1 1 42 RAW
	./${EXE} import datasources/msat2.json ${DATASET_C2} 1 2 42 RAW
	./${EXE} import datasources/msat2.json ${DATASET_C3} 1 3 42 RAW
	./${EXE} import datasources/msat2.json ${DATASET_C4} 1 4 42 RAW
	./${EXE} import datasources/msat2.json ${DATASET_C5} 1 5 42 RAW
	./${EXE} import datasources/msat2.json ${DATASET_C6} 1 6 42 RAW
	./${EXE} import datasources/msat2.json ${DATASET_C7} 1 7 42 RAW
	./${EXE} import datasources/msat2.json ${DATASET_C8} 1 8 42 RAW
	./${EXE} import datasources/msat2.json ${DATASET_C9} 1 9 42 RAW
	./${EXE} import datasources/msat2.json ${DATASET_C10} 1 10 42 RAW

tmeandatasource:	${EXE}
	rm -f datasources/tmean*.dat datasources/tmean*.db
	#./${EXE} createsource 3857 ${DATASET_TMEAN} >datasources/tmean.json
	./${EXE} import datasources/tmean.json ${DATASET_TMEAN} 1 0 42 RAW

worldclimdatasource:	${EXE}
	rm -f datasources/worldclim*.dat datasources/worldclim*.db
	./${EXE} import datasources/worldclim.json data/worldclim/tmin_1.bil 1 0 42 RAW
	./${EXE} import datasources/worldclim.json data/worldclim/tmax_1.bil 1 1 42 RAW
	./${EXE} import datasources/worldclim.json data/worldclim/tmean_1.bil 1 2 42 RAW
	./${EXE} import datasources/worldclim.json data/worldclim/prec_1.bil 1 3 42 RAW


test: ${EXE}
	valgrind --error-exitcode=4 ./${EXE} testquery queries/msat0.json

webinstall:
	php htdocs/get_css.php > htdocs/compiled/compiled.release.css
	php htdocs/get_javascript.php > htdocs/compiled/compiled.release.js
	php htdocs/get_templates.php > htdocs/compiled/compiled.release.soy.js

docs:
	doxygen Doxyfile

clean:
	rm -f o/*.o ${EXE} ${EXECGI} $(CL_HEADERS)
