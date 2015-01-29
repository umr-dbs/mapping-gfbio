#cerror "Don't compile me!"

#include "raster/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include <pqxx/pqxx>
#include <geos/geom/GeometryFactory.h>
#include <geos/io/WKBReader.h>
#include <string>
#include <sstream>


class PGPointSourceOperator : public GenericOperator {
	public:
		PGPointSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~PGPointSourceOperator();

		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect, QueryProfiler &profiler);
	private:
		pqxx::connection *connection;
};




PGPointSourceOperator::PGPointSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources), connection(nullptr) {
	assumeSources(0);

	const char *connectionstring = "host = 'localhost' dbname = 'idessa' user = 'idessa' password = 'idessa' "; // host = 'localhost'

	connection = new pqxx::connection(connectionstring);
}

PGPointSourceOperator::~PGPointSourceOperator() {
	delete connection;
	connection = nullptr;
}
REGISTER_OPERATOR(PGPointSourceOperator, "pggeometrysource");


std::unique_ptr<PointCollection> PGPointSourceOperator::getPoints(const QueryRectangle &rect, QueryProfiler &profiler) {

#define EPSG_AS_STRING2(e) #e
#define EPSG_AS_STRING(e) EPSG_AS_STRING2(e)
	const char *sql = "SELECT ST_AsBinary(ST_Transform(location, " EPSG_AS_STRING(EPSG_WEBMERCATOR) ")) FROM locations_export";
#undef EPSG_AS_STRING
#undef EPSG_AS_STRING2

	pqxx::work transaction(*connection, "load_points");

	pqxx::result points = transaction.exec(sql);


	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKBReader wkbreader(*gf);


	auto points_out = std::make_unique<PointCollection>();

	for (pqxx::result::size_type i=0; i < points.size();i++) {
		// location is points[i][0]
		//Process(R[i]["lastname"]);
		std::string rawstring = points[i][0].c_str();
		std::string wkb = rawstring.substr(2, std::string::npos);
		//printf("WKB: %s\n", wkb.c_str());

		std::istringstream wkb_stream(wkb);
		geos::geom::Geometry *point = wkbreader.readHEX(wkb_stream);

		if (point->getDimension() != 0 || point->getNumPoints() != 1) {
			printf("Point broken, skipping\n");
			continue; // memory leak..
		}

		const geos::geom::Coordinate *coords = point->getCoordinate();

		double x = coords->x, y = coords->y;

		points_out->addPoint(x, y);

		gf->destroyGeometry(point);
	}

	// TODO: capture I/O costs somehow
	return points_out;
}
