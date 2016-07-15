#include "datatypes/simplefeaturecollections/wkbutil.h"

#include "operators/operator.h"
#include "util/exceptions.h"
#include "util/curl.h"
#include "util/csvparser.h"
#include "util/configuration.h"
#include "util/make_unique.h"
#include "util/gfbiodatautil.h"
#include "datatypes/simplefeaturecollections/wkbutil.h"

#include <string>
#include <sstream>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/io/WKBReader.h>
#include <json/json.h>
#include "datatypes/pointcollection.h"
#include "datatypes/polygoncollection.h"
#include <json/json.h>
#include <pqxx/pqxx>

/**
 * This operator fetches GBIF occurrences directly from postgres. It should eventually be replaced by a
 * more generic vector source.
 */
class GFBioSourceOperator : public GenericOperator {
	public:
		GFBioSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~GFBioSourceOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual void getProvenance(ProvenanceCollection &pc);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		std::string scientificName;
		std::string dataSource;
		bool includeMetadata;
};


GFBioSourceOperator::GFBioSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);

	scientificName = params.get("scientificName", "").asString();
	dataSource = params.get("dataSource", "").asString();
	includeMetadata = params.get("includeMetadata", false).asBool();

	if(scientificName.length() < 3)
		throw ArgumentException("GBIFSourceOperator: scientificName must contain at least 3 characters");
}

GFBioSourceOperator::~GFBioSourceOperator() {
}
REGISTER_OPERATOR(GFBioSourceOperator, "gfbio_source");

void GFBioSourceOperator::writeSemanticParameters(std::ostringstream& stream) {
	Json::Value json(Json::objectValue);
	json["scientificName"] = scientificName;
	json["includeMetadata"] = includeMetadata;
	stream << json;
}

#ifndef MAPPING_OPERATOR_STUBS


void GFBioSourceOperator::getProvenance(ProvenanceCollection &pc) {
	if(dataSource == "GBIF") {
		pqxx::connection connection (Configuration::get("operators.gbifsource.dbcredentials"));

		std::string taxa = GFBioDataUtil::resolveTaxa(connection, scientificName);


		connection.prepare("provenance", "SELECT DISTINCT key, citation, uri from gbif.gbif_lite_time join gbif.gbif using (id) join gbif2.datasets ON (key = dataset_id) WHERE taxon = ANY($1)");
		pqxx::work work(connection);
		pqxx::result result = work.prepared("provenance")(taxa).exec();

		for(size_t i = 0; i < result.size(); ++i) {
			auto row = result[i];
			pc.add(Provenance(row[1].as<std::string>(), "", row[2].as<std::string>(), ""));
		}
	} else {
		pc.add(Provenance("IUCN 2014. The IUCN Red List of Threatened Species. Version 2014.1. http://www.iucnredlist.org. Downloaded on 06/01/2014.", "", "http://www.iucnredlist.org/", "http://spatial-data.s3.amazonaws.com/groups/Red%20List%20Terms%20&%20Conditions%20of%20Use.pdf"));
	}
}


std::unique_ptr<PointCollection> GFBioSourceOperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	//connect
	//TODO: reuse
	pqxx::connection connection (Configuration::get("operators.gbifsource.dbcredentials"));

	std::string taxa = GFBioDataUtil::resolveTaxa(connection, scientificName);

	//fetch occurrences
	auto points = make_unique<PointCollection>(rect);
	if(includeMetadata) {
		points->feature_attributes.addTextualAttribute("scientific_name", Unit::unknown());
		connection.prepare("occurrences", "SELECT ST_X(geom) lon, ST_Y(geom) lat, extract(epoch from gbif.gbif_lite_time.event_date), name as scientific_name from gbif.gbif_lite_time join gbif.gbif_taxon_to_name using (taxon) WHERE taxon = ANY($1) AND ST_CONTAINS(ST_MakeEnvelope($2, $3, $4, $5, 4326), geom)");
	}
	else
		connection.prepare("occurrences", "SELECT ST_X(geom) x, ST_Y(geom) y, extract(epoch from event_date) FROM gbif.gbif_lite_time WHERE taxon = ANY($1) AND ST_CONTAINS(ST_MakeEnvelope($2, $3, $4, $5, 4326), geom)");

	pqxx::work work(connection);
	pqxx::result result = work.prepared("occurrences")(taxa)(rect.x1)(rect.y1)(rect.x2)(rect.y2).exec();
    work.commit();

    //build feature collection
    //TODO: use cursor
    points->time.reserve(result.size());
    for(size_t i = 0; i < result.size(); ++i) {
    	auto row = result[i];
    	points->addSinglePointFeature(Coordinate(row[0].as<double>(), row[1].as<double>()));

//    	double t;
//    	if(row[2].is_null())
//    		t = rect.beginning_of_time();
//    	else
//    		t = row[2].as<double>();
//
//    	points->time.push_back(TimeInterval(t, rect.end_of_time()));

    	if(includeMetadata) {
    		points->feature_attributes.textual("scientific_name").set(i, row[3].as<std::string>());
    	}
    }
    //points->addDefaultTimestamps();

    return points;
}


std::unique_ptr<PolygonCollection> GFBioSourceOperator::getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	//connect
	//TODO: reuse
	pqxx::connection connection (Configuration::get("operators.gbifsource.dbcredentials"));

	std::string taxa = GFBioDataUtil::resolveTaxaNames(connection, scientificName);


	connection.prepare("occurrences", "SELECT ST_AsEWKT(ST_Collect(geom)) FROM iucn.expert_ranges_all WHERE lower(binomial) = ANY ($1)");

	pqxx::work work(connection);
	pqxx::result result = work.prepared("occurrences")(taxa).exec();
    work.commit();

    std::string wkt = result[0][0].as<std::string>();

    auto polygons = WKBUtil::readPolygonCollection(wkt, rect);

    return polygons;
}


#endif
