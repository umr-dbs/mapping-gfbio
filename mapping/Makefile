# GCC 4.8.1, -O2
# real    0m27.775s
# clang
# real    0m22.968s

LD_LIBRARY_PATH := /usr/local/lib/:$(LD_LIBRARY_PATH)
CPATH := /usr/local/include/:$(CPATH)

#CPP=g++
CPP=clang++
CPPFLAGS=-g -O2 -Wall -Wextra -pedantic-errors -std=c++11 -I. -I/usr/include/jsoncpp/ #-DMAPPING_NO_OPENCL
CPPFLAGS_R=-I/usr/local/lib/R/site-library/***REMOVED***/include/ -I/usr/local/lib/R/site-library/***REMOVED***/include/ -I/usr/share/R/include
LDFLAGS=-lgdal -ljsoncpp -lsqlite3 -lz -lbz2 -lpthread -pthread -lOpenCL -lpng -lturbojpeg -lpqxx -lgeos -lcurl -L/usr/lib/R/lib -lR -L/usr/local/lib/R/site-library/***REMOVED***/lib -l***REMOVED*** -Wl,-rpath,/usr/local/lib/R/site-library/***REMOVED***/lib
LDFLAGS_R=-L/usr/lib/R/lib -lR -L/usr/local/lib/R/site-library/***REMOVED***/lib -l***REMOVED*** -Wl,-rpath,/usr/local/lib/R/site-library/***REMOVED***/lib
LDFLAGS_CGI=-luriparser
CL_SOURCES:=$(wildcard operators/*/*.cl)
CL_HEADERS:=$(CL_SOURCES:.cl=.cl.h)
OBJ_RASTER=o/raster_raster.o o/raster_rastersource.o o/raster_colors.o o/raster_opencl.o o/raster_profiler.o o/raster_metadata.o o/raster_import_gdal.o o/raster_export_pgm.o o/raster_export_yuv.o o/raster_export_png.o o/raster_export_jpeg.o o/raster_pointcollection.o o/raster_geometry.o
OBJ_PLOT=o/plot_histogram.o o/plot_text.o
OBJ_UTIL=o/util_gdal.o o/util_hash.o o/util_curl.o o/util_sqlite.o o/util_sunpos.o o/util_binarystream.o o/util_csvparser.o
OBJ_POINTVISUALIZATION=o/pointvisualization_BoundingBox.o o/pointvisualization_Circle.o o/pointvisualization_Coordinate.o o/pointvisualization_Dimension.o o/pointvisualization_FindResult.o o/pointvisualization_QuadTreeNode.o o/pointvisualization_CircleClusteringQuadTree.o
OBJ_CONVERTERS=o/converters_converter.o o/converters_raw.o
OBJ_OPERATORS_RASTER=o/operators_raster_matrixkernel.o o/operators_raster_opencl.o o/operators_raster_expression.o o/operators_raster_projections.o o/operators_raster_rastersource.o o/operators_raster_r.o
OBJ_OPERATORS_POINTS=o/operators_points_pgpointsource.o o/operators_points_gfbiopointsource.o o/operators_points_points_filter_by_range.o o/operators_points_points_difference.o o/operators_points_points_cluster.o o/operators_points_csvpointsource.o
OBJ_OPERATORS_GEOMETRY=o/operators_geometry_testgeometrysource.o o/operators_geometry_osmgeometrysource.o
OBJ_OPERATORS_COMBINED=o/operators_combined_points2raster.o o/operators_combined_raster_metadata_to_points.o o/operators_combined_points2cluster_raster.o o/operators_combined_filterpointsbygeometry.o
OBJ_OPERATORS_MSAT=o/operators_msat_temperature.o o/operators_msat_reflectance.o o/operators_msat_solarangle.o o/operators_msat_sofos_cloud_classification.o
OBJ_OPERATORS_PLOTS=o/operators_plots_histogram.o o/operators_plots_points2histogram.o o/operators_plots_points_metadata_tograph.o
OBJ_OPERATORS=o/operators_operator.o $(OBJ_OPERATORS_RASTER) $(OBJ_OPERATORS_POINTS) $(OBJ_OPERATORS_GEOMETRY) $(OBJ_OPERATORS_COMBINED) $(OBJ_OPERATORS_MSAT) $(OBJ_OPERATORS_PLOTS)
OBJ_COMMON=${OBJ_RASTER} ${OBJ_CONVERTERS} ${OBJ_PLOT} ${OBJ_POINTVISUALIZATION} ${OBJ_OPERATORS} ${OBJ_UTIL}
OBJ_EXE=o/main_rasterdb.o ${OBJ_COMMON}
OBJ_EXECGI=o/main_cgi.o ${OBJ_COMMON}
# TODO: more fine grained selection of the required objects
OBJ_RSERVER=o/rserver_rserver.o o/operators_operator.o ${OBJ_CONVERTERS} ${OBJ_RASTER} ${OBJ_UTIL}
EXE=mapping_manager
EXECGI=../htdocs/cgi-bin/mapping
EXERSERVER=r_server

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
DATASET_HRV=data/xrit2/H-000-MSG2__-MSG2________-HRV______-000001___-201006061200-C_

DATASET2_C0=data/xrit2/H-000-MSG2__-MSG2________-VIS006___-000001___-201006061800-C_
DATASET2_C1=data/xrit2/H-000-MSG2__-MSG2________-VIS008___-000001___-201006061800-C_
DATASET2_C2=data/xrit2/H-000-MSG2__-MSG2________-IR_016___-000001___-201006061800-C_
DATASET2_C3=data/xrit2/H-000-MSG2__-MSG2________-IR_039___-000001___-201006061800-C_
DATASET2_C4=data/xrit2/H-000-MSG2__-MSG2________-WV_062___-000001___-201006061800-C_
DATASET2_C5=data/xrit2/H-000-MSG2__-MSG2________-WV_073___-000001___-201006061800-C_
DATASET2_C6=data/xrit2/H-000-MSG2__-MSG2________-IR_087___-000001___-201006061800-C_
DATASET2_C7=data/xrit2/H-000-MSG2__-MSG2________-IR_097___-000001___-201006061800-C_
DATASET2_C8=data/xrit2/H-000-MSG2__-MSG2________-IR_108___-000001___-201006061800-C_
DATASET2_C9=data/xrit2/H-000-MSG2__-MSG2________-IR_120___-000001___-201006061800-C_
DATASET2_C10=data/xrit2/H-000-MSG2__-MSG2________-IR_134___-000001___-201006061800-C_
DATASET2_HRV=data/xrit2/H-000-MSG2__-MSG2________-HRV______-000001___-201006061800-C_

.SECONDARY: $(CL_HEADERS)

.DELETE_ON_ERROR:

operators/%.cl.h: operators/%.cl
	xxd -i $+ | sed 's/unsigned /static const /g' > $@
	echo $+ | sed -e 's/\.cl$$//' -e 's:/:_:g' -e 's/\(^.*$$\)/static const std::string \1(\1_cl, \1_cl_len);/' >> $@ 

o/util_%.o: util/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/pointvisualization_%.o: pointvisualization/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/raster_%.o: raster/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/converters_%.o: converters/%.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@

o/plot_%.o: plot/%.cpp
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


o/main_%.o: %.cpp
	${CPP} ${CPPFLAGS} -c $+ -o $@


all: ${EXE} ${EXECGI} ${EXERSERVER}

${EXE}:	${OBJ_EXE}
	${CPP} $+ ${LDFLAGS} -o $@

${EXECGI}:	${OBJ_EXECGI}
	${CPP} $+ ${LDFLAGS} ${LDFLAGS_CGI} -o $@

${EXERSERVER}:	${OBJ_RSERVER}
	${CPP} ${LDFLAGS} ${LDFLAGS_R} $+ -o $@

	
msg9_datasource:	${EXE}
	rm -f datasources/msg9_geos.dat datasources/msg9_geos.db
	#./${EXE} createsource 62866 ${DATASET_C0} ${DATASET_C1} ${DATASET_C2} ${DATASET_C3} ${DATASET_C4} ${DATASET_C5} ${DATASET_C6} ${DATASET_C7} ${DATASET_C8} ${DATASET_C9} ${DATASET_C10} >datasources/msat2.json
	./${EXE} import datasources/msg9_geos.json ${DATASET_C0} 1 0 42 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET_C1} 1 1 42 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET_C2} 1 2 42 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET_C3} 1 3 42 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET_C4} 1 4 42 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET_C5} 1 5 42 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET_C6} 1 6 42 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET_C7} 1 7 42 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET_C8} 1 8 42 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET_C9} 1 9 42 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET_C10} 1 10 42 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET2_C0} 1 0 43 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET2_C1} 1 1 43 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET2_C2} 1 2 43 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET2_C3} 1 3 43 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET2_C4} 1 4 43 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET2_C5} 1 5 43 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET2_C6} 1 6 43 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET2_C7} 1 7 43 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET2_C8} 1 8 43 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET2_C9} 1 9 43 RAW
	./${EXE} import datasources/msg9_geos.json ${DATASET2_C10} 1 10 43 RAW

msg9_hrvdatasource:	${EXE}
	rm -f datasources/msg9_geos_hrv.dat datasources/msg9_geos_hrv.db
	./${EXE} import datasources/msg9_geos_hrv.json ${DATASET_HRV} 1 0 42 RAW
	./${EXE} import datasources/msg9_geos_hrv.json ${DATASET2_HRV} 1 0 43 RAW

worldclimdatasource:	${EXE}
	rm -f datasources/worldclim*.dat datasources/worldclim*.db
	./${EXE} import datasources/worldclim.json data/worldclim/tmin_1.bil 1 0 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/tmax_1.bil 1 1 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/tmean_1.bil 1 2 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/prec_1.bil 1 3 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/alt.bil 1 4 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_1.bil 1 5 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_2.bil 1 6 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_3.bil 1 7 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_4.bil 1 8 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_5.bil 1 9 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_6.bil 1 10 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_7.bil 1 11 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_8.bil 1 12 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_9.bil 1 13 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_10.bil 1 14 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_11.bil 1 15 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_12.bil 1 16 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_13.bil 1 17 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_14.bil 1 18 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_15.bil 1 19 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_16.bil 1 20 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_17.bil 1 21 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_18.bil 1 22 42 GZIP
	./${EXE} import datasources/worldclim.json data/worldclim/bio_19.bil 1 23 42 GZIP

srtmdatasource:	${EXE}
	rm -f datasources/srtm.dat datasources/srtm.db
	./${EXE} import datasources/srtm.json data/srtm/srtm_all_index.out 1 0 42 GZIP

test: ${EXE}
	valgrind --error-exitcode=4 ./${EXE} testquery queries/msat0.json

webinstall:
	php htdocs/get_css.php > htdocs/compiled/compiled.release.css
	php htdocs/get_javascript.php > htdocs/compiled/compiled.release.js
	php htdocs/get_templates.php > htdocs/compiled/compiled.release.soy.js

docs:
	doxygen Doxyfile

clean:
	rm -f o/*.o ${EXE} ${EXECGI} ${EXERSERVER} $(CL_HEADERS)
