#include "datatypes/multipointcollection.h"
#include "operators/operator.h"
#include "raster/exceptions.h"
#include "util/make_unique.h"
#include "util/configuration.h"

#include <pqxx/pqxx>
#include <string>
#include <sstream>
#include <json/json.h>


class PGPointSourceOperator : public GenericOperator {
	public:
		PGPointSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~PGPointSourceOperator();

		virtual std::unique_ptr<MultiPointCollection> getMultiPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);

	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		std::string connectionstring;
		std::string querystring;
		pqxx::connection *connection;
};




PGPointSourceOperator::PGPointSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources), connection(nullptr) {
	assumeSources(0);

	connectionstring = params.get("connection", Configuration::get("operators.pgpointsource.dbcredentials", "")).asString();
	querystring = params.get("query", "x, y FROM locations").asString();

	connection = new pqxx::connection(connectionstring);
}

PGPointSourceOperator::~PGPointSourceOperator() {
	delete connection;
	connection = nullptr;
}
REGISTER_OPERATOR(PGPointSourceOperator, "pgpointsource");

void PGPointSourceOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "\"querystring\":\"" << querystring << "\"";
}

std::unique_ptr<MultiPointCollection> PGPointSourceOperator::getMultiPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {

	if (rect.epsg != EPSG_WEBMERCATOR)
		throw OperatorException("PGPointSourceOperator: Shouldn't load points in a projection other than webmercator");

	std::stringstream sql;
	sql << "SELECT " << querystring << " WHERE x >= " << std::min(rect.x1,rect.x2) << " AND x <= " << std::max(rect.x1,rect.x2) << " AND y >= " << std::min(rect.y1,rect.y2) << " AND y <= " << std::max(rect.y1,rect.y2);

	pqxx::work transaction(*connection, "load_points");
	pqxx::result points = transaction.exec(sql.str());

	auto points_out = std::make_unique<MultiPointCollection>(rect);

	auto column_count = points.columns();
	for (pqxx::result::size_type c = 2;c<column_count;c++) {
		points_out->local_md_value.addVector(points.column_name(c));
	}

	for (pqxx::result::tuple::size_type i=0; i < points.size();i++) {
		auto row = points[i];
		double x = row[0].as<double>(),
		       y = row[1].as<double>();


		size_t idx = points_out->addFeature(Coordinate(x, y));
		for (pqxx::result::tuple::size_type c = 2;c<column_count;c++) {
			points_out->local_md_value.set(idx, points.column_name(c), row[c].as<double>());
		}
	}

	// TODO: capture I/O cost somehow

	return points_out;
}
