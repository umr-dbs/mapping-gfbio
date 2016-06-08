#include "datatypes/simplefeaturecollections/wkbutil.h"

#include "operators/operator.h"
#include "util/exceptions.h"
#include "util/curl.h"
#include "util/csvparser.h"
#include "util/configuration.h"
#include "util/make_unique.h"

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
class GBIFSourceOperator : public GenericOperator {
	public:
		GBIFSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~GBIFSourceOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual void getProvenance(ProvenanceCollection &pc);

		std::string resolveTaxa(pqxx::connection &connection);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		std::string scientificName;
		bool includeMetadata;
};


GBIFSourceOperator::GBIFSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);

	scientificName = params.get("scientificName", "").asString();
	includeMetadata = params.get("includeMetadata", false).asBool();

	if(scientificName.length() < 3)
		throw ArgumentException("GBIFSourceOperator: scientificName must contain at least 3 characters");
}

GBIFSourceOperator::~GBIFSourceOperator() {
}
REGISTER_OPERATOR(GBIFSourceOperator, "gbif_source");

void GBIFSourceOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "{";
	stream << "\"scientificName\":\"" << scientificName << "\",";
	stream << "\"includeMetadata\":\"" << includeMetadata << "\",";
	stream << "}";
}

#ifndef MAPPING_OPERATOR_STUBS

std::string GBIFSourceOperator::resolveTaxa(pqxx::connection &connection) {
	connection.prepare("taxa", "SELECT DISTINCT taxon FROM gbif.gbif_taxon_to_name WHERE name ILIKE $1");
	pqxx::work work(connection);
	pqxx::result result = work.prepared("taxa")(scientificName + "%").exec();

	std::stringstream taxa;
	taxa << "{";
	for(size_t i = 0; i < result.size(); ++i) {
		if(i != 0)
			taxa << ",";
		taxa << result[i][0];
	}
	taxa << "}";
	return taxa.str();
}

void GBIFSourceOperator::getProvenance(ProvenanceCollection &pc) {
	pqxx::connection connection (Configuration::get("operators.gbifsource.dbcredentials"));

	std::string taxa = resolveTaxa(connection);


	connection.prepare("provenance", "SELECT DISTINCT key, citation, uri from gbif.gbif_lite_time join gbif.gbif using (id) join gbif2.datasets ON (key = dataset_id) WHERE taxon = ANY($1)");
	pqxx::work work(connection);
	pqxx::result result = work.prepared("provenance")(taxa).exec();

	for(size_t i = 0; i < result.size(); ++i) {
		auto row = result[i];
		pc.add(Provenance(row[1].as<std::string>(), "", row[2].as<std::string>(), ""));
	}
}


std::unique_ptr<PointCollection> GBIFSourceOperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	//connect
	//TODO: reuse
	pqxx::connection connection (Configuration::get("operators.gbifsource.dbcredentials"));

	std::string taxa = resolveTaxa(connection);

	//fetch occurrences
	auto points = make_unique<PointCollection>(rect);
	if(includeMetadata) {
		points->feature_attributes.addTextualAttribute("scientific_name", Unit::unknown());
		connection.prepare("occurrences", "SELECT ST_X(geom) lon, ST_Y(geom) lat, extract(epoch from gbif.gbif_lite_time.event_date), scientific_name from gbif.gbif_lite_time join gbif.gbif using (id) WHERE taxon = ANY($1) AND ST_CONTAINS(ST_MakeEnvelope($2, $3, $4, $5, 4326), geom)");
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

    	double t;
    	if(row[2].is_null())
    		t = rect.beginning_of_time();
    	else
    		t = row[2].as<double>();

    	points->time.push_back(TimeInterval(t, rect.end_of_time()));

    	if(includeMetadata) {
    		points->feature_attributes.textual("scientific_name").set(i, row[3].as<std::string>());
    	}
    }

    return points;
}

#endif
