
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
OBJS_CORE += o/core/rasterdb/rasterdb.o o/core/rasterdb/backend.o o/core/rasterdb/backend_local.o
OBJS_CORE += o/core/rasterdb/converters/converter.o o/core/rasterdb/converters/raw.o
# userdb: move to core for now
OBJS_CORE += o/core/userdb/userdb.o o/core/userdb/backend_sqlite.o
OBJS_CORE += o/core/util/gdal.o o/core/util/sha1.o o/core/util/curl.o o/core/util/sqlite.o o/core/util/binarystream.o o/core/util/csvparser.o o/core/util/base64.o o/core/util/configuration.o o/core/util/formula.o o/core/util/debug.o o/core/util/timemodification.o o/core/util/log.o o/core/util/timeparser.o o/core/util/sizeutil.o
# operator and query base must be in core for now
OBJS_CORE += o/core/operators/operator.o o/core/operators/provenance.o o/core/operators/queryrectangle.o o/core/operators/queryprofiler.o

#
# HTTP Services
#
OBJS_SERVICES += o/core/services/httpservice.o o/core/services/httpparsing.o o/core/services/user.o o/core/services/ogcservice.o o/core/services/wms.o o/core/services/wcs.o o/core/services/wfs.o o/core/services/plot.o o/core/services/provenance.o o/core/services/artifact.o
# pointvisualization is only used by services, so that's where it goes
OBJS_SERVICES += o/core/pointvisualization/BoundingBox.o o/core/pointvisualization/Circle.o o/core/pointvisualization/Coordinate.o o/core/pointvisualization/Dimension.o o/core/pointvisualization/FindResult.o o/core/pointvisualization/QuadTreeNode.o o/core/pointvisualization/CircleClusteringQuadTree.o

#
# Operators
#
OBJS_OPERATORS += o/core/operators/source/csv_source.o o/core/operators/source/postgres_source.o o/core/operators/source/rasterdb_source.o o/core/operators/source/wkt_source.o
OBJS_OPERATORS += o/core/operators/processing/raster/matrixkernel.o o/core/operators/processing/raster/expression.o o/core/operators/processing/raster/classification.o
OBJS_OPERATORS += o/core/operators/processing/features/difference.o o/core/operators/processing/features/numeric_attribute_filter.o o/core/operators/processing/features/point_in_polygon_filter.o
OBJS_OPERATORS += o/core/operators/processing/combined/projection.o o/core/operators/processing/combined/raster_value_extraction.o o/core/operators/processing/combined/rasterization.o o/core/operators/processing/combined/timeshift.o
OBJS_OPERATORS += o/core/operators/processing/meteosat/temperature.o o/core/operators/processing/meteosat/reflectance.o o/core/operators/processing/meteosat/solarangle.o o/core/operators/processing/meteosat/radiance.o o/core/operators/processing/meteosat/pansharpening.o o/core/operators/processing/meteosat/gccthermthresholddetection.o o/core/operators/processing/meteosat/co2correction.o
OBJS_OPERATORS += o/core/util/sunpos.o
OBJS_OPERATORS += o/core/operators/plots/histogram.o o/core/operators/plots/feature_attributes_plot.o
OBJS_OPERATORS += o/core/datatypes/plots/histogram.o o/core/datatypes/plots/text.o o/core/datatypes/plots/png.o


# Cache: needs to be core for now
OBJS_CORE += o/core/cache/common.o o/core/cache/priv/shared.o o/core/cache/priv/requests.o o/core/cache/priv/connection.o o/core/cache/priv/redistribution.o o/core/cache/priv/cache_stats.o o/core/cache/priv/cache_structure.o o/core/cache/node/node_cache.o o/core/cache/manager.o o/core/cache/priv/caching_strategy.o o/core/cache/priv/cube.o


#
# GTest
#
SRC_GTEST:=$(wildcard test/unittests/*.cpp) $(wildcard test/unittests/*/*.cpp)
OBJS_GTEST += $(patsubst test/%,o/core/test/%,$(SRC_GTEST:.cpp=.o))
# and a few requirements
OBJS_GTEST += o/core/util/server_nonblocking.o

# now define all binaries we wish to create
ALL_TARGETS += MANAGER MANAGER_SAN CGI GTEST PARSETESTLOGS

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
