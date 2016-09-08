#include "operators/operator.h"
#include "util/exceptions.h"
#include "util/make_unique.h"
#include "util/configuration.h"

#include <pqxx/pqxx>
#include <string>
#include <sstream>
#include <json/json.h>
#include "datatypes/pointcollection.h"

/**
 * Operator that retrieves points from a postgres database
 *
 * Parameters:
 * - connection: a string containing the connection information for the database
 * - query: an SQL query
 */
class PostgresSourceOperator : public GenericOperator {
	public:
		PostgresSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~PostgresSourceOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, const QueryTools &tools);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		std::string connectionstring;
		std::string querystring;
#ifndef MAPPING_OPERATOR_STUBS
		std::unique_ptr<pqxx::connection> connection;
#endif
};




PostgresSourceOperator::PostgresSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);

	connectionstring = params.get("connection", Configuration::get("operators.pgpointsource.dbcredentials", "")).asString();
	querystring = params.get("query", "x, y FROM locations").asString();

#ifndef MAPPING_OPERATOR_STUBS
	connection = make_unique<pqxx::connection>(connectionstring);
#endif
}

PostgresSourceOperator::~PostgresSourceOperator() {
}
REGISTER_OPERATOR(PostgresSourceOperator, "postgres_source");

void PostgresSourceOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "{\"querystring\":\"" << querystring << "\"}";
}

#ifndef MAPPING_OPERATOR_STUBS
std::unique_ptr<PointCollection> PostgresSourceOperator::getPointCollection(const QueryRectangle &rect, const QueryTools &tools) {

	if (rect.epsg != EPSG_WEBMERCATOR)
		throw OperatorException("PGPointSourceOperator: Shouldn't load points in a projection other than webmercator");

	std::stringstream sql;
	sql << "SELECT " << querystring << " WHERE x >= " << std::min(rect.x1,rect.x2) << " AND x <= " << std::max(rect.x1,rect.x2) << " AND y >= " << std::min(rect.y1,rect.y2) << " AND y <= " << std::max(rect.y1,rect.y2);

	pqxx::work transaction(*connection, "load_points");
	pqxx::result points = transaction.exec(sql.str());

	auto points_out = make_unique<PointCollection>(rect);

	auto column_count = points.columns();
	for (pqxx::result::size_type c = 2;c<column_count;c++) {
		points_out->feature_attributes.addNumericAttribute(points.column_name(c), Unit::unknown());
	}

	for (pqxx::result::tuple::size_type i=0; i < points.size();i++) {
		auto row = points[i];
		double x = row[0].as<double>(),
		       y = row[1].as<double>();


		size_t idx = points_out->addSinglePointFeature(Coordinate(x, y));
		for (pqxx::result::tuple::size_type c = 2;c<column_count;c++) {
			points_out->feature_attributes.numeric(points.column_name(c)).set(idx, row[c].as<double>());
		}
	}

	// TODO: capture I/O cost somehow

	return points_out;
}
#endif
