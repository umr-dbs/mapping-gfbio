#include "raster/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include <pqxx/pqxx>
#include <string>
#include <sstream>
#include <json/json.h>


class PGPointSourceOperator : public GenericOperator {
	public:
		PGPointSourceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~PGPointSourceOperator();

		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
	private:
		std::string connectionstring;
		std::string querystring;
		pqxx::connection *connection;
};




PGPointSourceOperator::PGPointSourceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::RASTER, sourcecount, sources), connection(nullptr) {
	assumeSources(0);

	connectionstring = params.get("connection", "host = 'localhost' dbname = 'idessa' user = 'idessa' password = 'idessa' ").asString();
	querystring = params.get("query", "x, y FROM locations").asString();

	//const char *connectionstring = "host = 'localhost' dbname = 'idessa' user = 'idessa' password = 'idessa' "; // host = 'localhost'

	connection = new pqxx::connection(connectionstring);
}

PGPointSourceOperator::~PGPointSourceOperator() {
	delete connection;
	connection = nullptr;
}
REGISTER_OPERATOR(PGPointSourceOperator, "pgpointsource");


std::unique_ptr<PointCollection> PGPointSourceOperator::getPoints(const QueryRectangle &rect) {

	if (rect.epsg != EPSG_WEBMERCATOR)
		throw OperatorException("PGPointSourceOperator: Shouldn't load points in a projection other than webmercator");

	std::stringstream sql;
	sql << "SELECT " << querystring << " WHERE x >= " << std::min(rect.x1,rect.x2) << " AND x <= " << std::max(rect.x1,rect.x2) << " AND y >= " << std::min(rect.y1,rect.y2) << " AND y <= " << std::max(rect.y1,rect.y2);

	pqxx::work transaction(*connection, "load_points");
	pqxx::result points = transaction.exec(sql.str());

	auto points_out = std::make_unique<PointCollection>(EPSG_WEBMERCATOR);

	auto column_count = points.columns();
	for (pqxx::result::size_type c = 2;c<column_count;c++) {
		points_out->addLocalMDValue(points.column_name(c));
	}

	for (pqxx::result::tuple::size_type i=0; i < points.size();i++) {
		auto row = points[i];
		double x = row[0].as<double>(),
		       y = row[1].as<double>();


		Point &point = points_out->addPoint(x, y);
		for (pqxx::result::tuple::size_type c = 2;c<column_count;c++) {
			points_out->setLocalMDValue(point, points.column_name(c), row[c].as<double>());
		}
	}

	return points_out;
}
