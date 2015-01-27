#include "datatypes/geometry.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include <geos/geom/GeometryFactory.h>
#include <geos/io/WKTReader.h>
#include <string>
#include <sstream>


class TestGeometrySourceOperator : public GenericOperator {
	public:
		TestGeometrySourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~TestGeometrySourceOperator();

		virtual std::unique_ptr<GenericGeometry> getGeometry(const QueryRectangle &rect, QueryProfiler &profiler);
};




TestGeometrySourceOperator::TestGeometrySourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);
}

TestGeometrySourceOperator::~TestGeometrySourceOperator() {
}
REGISTER_OPERATOR(TestGeometrySourceOperator, "testgeometrysource");


std::unique_ptr<GenericGeometry> TestGeometrySourceOperator::getGeometry(const QueryRectangle &rect, QueryProfiler &profiler) {

	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKTReader wktreader(*gf);

	std::string data(
		"GEOMETRYCOLLECTION("
		"POINT(6 10),\n"
		"LINESTRING(3 4,10 50,20 25),\n"
		"POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2,2 3,3 3,3 2,2 2)),\n"
		"MULTIPOINT((3.5 5.6), (4.8 10.5)),\n"
		"MULTILINESTRING((3 4,10 50,20 25),(-5 -8,-10 -8,-15 -4)),\n"
		"MULTIPOLYGON(((1 1,5 1,5 5,1 5,1 1),(2 2,2 3,3 3,3 2,2 2)),((6 3,9 2,9 4,6 3))),\n"
		"GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6,7 10)),\n"
		"POINT ZM (1 1 5 60),\n"
		"POINT M (1 1 80),\n"
		//"POINT EMPTY,\n"
		"MULTIPOLYGON EMPTY\n"
		")"
	);

	geos::geom::Geometry *geom = wktreader.read(data);

	auto geom_out = std::make_unique<GenericGeometry>(rect);
	geom_out->setGeom(geom);

	return geom_out;
}
