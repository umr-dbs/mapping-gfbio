#include "raster/geometry.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include <pqxx/pqxx>
#include <geos/geom/GeometryFactory.h>
#include <geos/io/WKTReader.h>
#include <string>
#include <sstream>


class OSMGeometrySourceOperator : public GenericOperator {
	public:
		OSMGeometrySourceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~OSMGeometrySourceOperator();

		virtual std::unique_ptr<GenericGeometry> getGeometry(const QueryRectangle &rect);
	private:
		std::string connectionstring;
		std::string querystring;
		pqxx::connection *connection;
};




OSMGeometrySourceOperator::OSMGeometrySourceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(0);

	connectionstring = "host = 'localhost' dbname = 'gfbio' user = 'gfbio' password = '***REMOVED***'";


	connection = new pqxx::connection(connectionstring);
}

OSMGeometrySourceOperator::~OSMGeometrySourceOperator() {
}
REGISTER_OPERATOR(OSMGeometrySourceOperator, "osmgeometrysource");


std::unique_ptr<GenericGeometry> OSMGeometrySourceOperator::getGeometry(const QueryRectangle &rect) {
	fprintf(stderr,"MMStart");
	std::string sql ="SELECT ST_AsEWKT(ST_Collect(geom)) FROM osm.roads;";
	auto geom_out = std::make_unique<GenericGeometry>(EPSG_LATLON);

	try {
		pqxx::work transaction(*connection, "load_points");
		fprintf(stderr,"MMTransactionStart");
		pqxx::result result = transaction.exec(sql);

		fprintf(stderr,"MMTransactionExec");


		auto column_count = result.columns();

		auto row = result[0];
		fprintf(stderr,"testetsstAAA");
		std::stringstream wkt(row[0].as<std::string>());
		fprintf(stderr,wkt.str().c_str());
		const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
		geos::io::WKTReader wktreader(*gf);

		geos::geom::Geometry *geom = wktreader.read(wkt.str());

		geom_out->setGeom(geom);
	}
	catch(const pqxx::pqxx_exception &e){
		fprintf(stderr, e.base().what());
	}
	return geom_out;
}
