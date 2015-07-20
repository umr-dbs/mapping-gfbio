#include "operators/operator.h"
#include "util/make_unique.h"

#include <pqxx/pqxx>
#include <geos/geom/GeometryFactory.h>
#include <geos/io/WKTReader.h>
#include <string>
#include <sstream>
#include "datatypes/linecollection.h"


class OSMGeometrySourceOperator : public GenericOperator {
	public:
		OSMGeometrySourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~OSMGeometrySourceOperator();

		//virtual std::unique_ptr<GenericGeometry> getGeometry(const QueryRectangle &rect, QueryProfiler &profiler);
	protected:
		void writeSemanticParameters(std::ostringstream& stream);
	private:
		std::string connectionstring;
		std::string querystring;
		pqxx::connection *connection;
};




OSMGeometrySourceOperator::OSMGeometrySourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);

	connectionstring = "host = 'localhost' dbname = 'gfbio' user = 'gfbio' password = '***REMOVED***'";


	connection = new pqxx::connection(connectionstring);
}

OSMGeometrySourceOperator::~OSMGeometrySourceOperator() {
}
REGISTER_OPERATOR(OSMGeometrySourceOperator, "osmgeometrysource");

void OSMGeometrySourceOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "\"querystring\":\"" << querystring << "\"";
}

//TODO: migrate to new simplefeaturecollection

//std::unique_ptr<GenericGeometry> OSMGeometrySourceOperator::getGeometry(const QueryRectangle &rect, QueryProfiler &profiler) {
//	fprintf(stderr,"MMStart");
//	std::string sql ="SELECT ST_AsEWKT(ST_Collect(geom)) FROM osm.roads;";
//	auto geom_out = make_unique<GenericGeometry>(rect);
//
//	try {
//		pqxx::work transaction(*connection, "load_points");
//		fprintf(stderr,"MMTransactionStart");
//		pqxx::result result = transaction.exec(sql);
//
//		fprintf(stderr,"MMTransactionExec");
//
//
//		auto column_count = result.columns();
//
//		auto row = result[0];
//		fprintf(stderr,"testetsstAAA");
//		std::stringstream wkt(row[0].as<std::string>());
//		fprintf(stderr,wkt.str().c_str());
//		const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
//		geos::io::WKTReader wktreader(*gf);
//
//		geos::geom::Geometry *geom = wktreader.read(wkt.str());
//
//		geom_out->setGeom(geom);
//	}
//	catch(const pqxx::pqxx_exception &e){
//		fprintf(stderr, e.base().what());
//	}
//	// TODO: capture I/O costs somehow
//	return geom_out;
//}
