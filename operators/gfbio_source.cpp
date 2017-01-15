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
#include <math.h>

/**
 * This operator fetches GBIF occurrences and IUCN expert rangesdirectly from postgres. It should eventually be replaced by a
 * more generic vector source.
 *
 * - Parameters:
 * 	- dataSource: gbif | iucn
 * 	- scientificName: the name of the species
 * 	- columns:
 * 		- numeric: array of column names of numeric type
 * 		- textual: array of column names of textual type
 */
class GFBioSourceOperator : public GenericOperator {
	public:
		GFBioSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~GFBioSourceOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, const QueryTools &tools);
		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, const QueryTools &tools);
		virtual void getProvenance(ProvenanceCollection &pc);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		std::string scientificName;
		std::string dataSource;

		std::vector<std::string> numeric_attributes;
		std::vector<std::string> textual_attributes;

		const std::set<std::string> gbif_columns {"gbifid", "datasetkey", "occurrenceid", "kingdom", "phylum", "class", "order", "family", "genus", "species", "infraspecificepithet", "taxonrank", "scientificname", "countrycode", "locality", "publishingorgkey", "decimallatitude", "decimallongitude", "coordinateuncertaintyinmeters", "coordinateprecision", "elevation", "elevationaccuracy", "depth", "depthaccuracy", "eventdate", "day", "month", "year", "taxonkey", "specieskey", "basisofrecord", "institutioncode", "collectioncode", "catalognumber", "recordnumber", "identifiedby", "license", "rightsholder", "recordedby", "typestatus", "establishmentmeans", "lastinterpreted", "mediatype", "issue"};
};


GFBioSourceOperator::GFBioSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);

	scientificName = params.get("scientificName", "").asString();
	dataSource = params.get("dataSource", "").asString();

	if(scientificName.length() < 3)
		throw ArgumentException("GFBioSourceOperator: scientificName must contain at least 3 characters");

	// attributes to be extracted
	if(!params.isMember("columns") || !params["columns"].isObject())
		throw ArgumentException("GFBioSourceOperator: columns are not specified");

	auto columns = params["columns"];
	if(!columns.isMember("numeric") || !columns["numeric"].isArray())
		throw ArgumentException("GFBioSourceOperator: numeric columns are not specified");

	if(!columns.isMember("textual") || !columns["textual"].isArray())
		throw ArgumentException("GFBioSourceOperator: textual columns are not specified");

	for(auto &attribute : columns["numeric"])
		numeric_attributes.push_back(attribute.asString());

	for(auto &attribute : columns["textual"])
		textual_attributes.push_back(attribute.asString());
}

GFBioSourceOperator::~GFBioSourceOperator() {
}
REGISTER_OPERATOR(GFBioSourceOperator, "gfbio_source");

void GFBioSourceOperator::writeSemanticParameters(std::ostringstream& stream) {
	Json::Value json(Json::objectValue);
	json["scientificName"] = scientificName;

	Json::Value columns(Json::objectValue);

	Json::Value jsonNumeric(Json::arrayValue);
	for (auto &attribute : numeric_attributes)
		jsonNumeric.append(attribute);
	columns["numeric"] = jsonNumeric;

	Json::Value jsonTextual(Json::arrayValue);
	for (auto &attribute : textual_attributes)
		jsonTextual.append(attribute);
	columns["textual"] = jsonTextual;

	json["columns"] = columns;

	stream << json;
}

#ifndef MAPPING_OPERATOR_STUBS


void GFBioSourceOperator::getProvenance(ProvenanceCollection &pc) {
	if(dataSource == "GBIF") {
		pqxx::connection connection (Configuration::get("operators.gfbiosource.dbcredentials"));

		std::string taxa = GFBioDataUtil::resolveTaxa(connection, scientificName);


		connection.prepare("provenance", "SELECT DISTINCT key, citation, uri from gbif.gbif_lite_time join gbif.datasets ON (uid = key) WHERE taxon = ANY($1)");
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


std::unique_ptr<PointCollection> GFBioSourceOperator::getPointCollection(const QueryRectangle &rect, const QueryTools &tools) {
	//connect
	//TODO: reuse
	pqxx::connection connection (Configuration::get("operators.gfbiosource.dbcredentials"));

	std::string taxa = GFBioDataUtil::resolveTaxa(connection, scientificName);

	//fetch occurrences
	auto points = make_unique<PointCollection>(rect);
	if(textual_attributes.size() > 0 || numeric_attributes.size() > 0) {
		std::stringstream columns;

		// add attributes to collection and build query string
		for(auto &attribute : numeric_attributes) {
			if(gbif_columns.find(attribute) == gbif_columns.end())
				throw ArgumentException("GFBioSource: Invalid column name: " + attribute);

			points->feature_attributes.addNumericAttribute(attribute, Unit::unknown());

			columns << ", \"" << connection.esc(attribute) << "\"";
		}

		for(auto &attribute : textual_attributes) {
			if(gbif_columns.find(attribute) == gbif_columns.end())
				throw ArgumentException("GFBioSource: Invalid column name: " + attribute);

			points->feature_attributes.addTextualAttribute(attribute, Unit::unknown());

			columns << ", \"" << connection.esc(attribute) <<"\"";
		}

		std::string query =
				"SELECT decimallongitude::double precision, decimallatitude::double precision, extract(epoch from eventdate)"
						+ columns.str()
						+ " from gbif.gbif WHERE taxonkey = ANY($1) AND ST_CONTAINS(ST_MakeEnvelope($2, $3, $4, $5, 4326), ST_SetSRID(ST_MakePoint(decimallongitude::double precision, decimallatitude::double precision),4326))";

		connection.prepare("occurrences", query);
	}
	else
		connection.prepare("occurrences", "SELECT ST_X(geom) x, ST_Y(geom) y, extract(epoch from eventdate) FROM gbif.gbif_lite_time WHERE taxon = ANY($1) AND ST_CONTAINS(ST_MakeEnvelope($2, $3, $4, $5, 4326), geom)");

	pqxx::work work(connection);
	pqxx::result result = work.prepared("occurrences")(taxa)(rect.x1)(rect.y1)(rect.x2)(rect.y2).exec();
    work.commit();

    //build feature collection
    //TODO: use cursor
    points->time.reserve(result.size());
    for(size_t i = 0; i < result.size(); ++i) {
    	auto row = result[i];
    	points->addSinglePointFeature(Coordinate(row[0].as<double>(), row[1].as<double>()));

    	// TODO: include time again when rasterValueExtraction works as expected
//    	double t;
//    	if(row[2].is_null())
//    		t = rect.beginning_of_time();
//    	else
//    		t = row[2].as<double>();
//
//    	points->time.push_back(TimeInterval(t, rect.end_of_time()));

    	// attributes
    	for(auto &attribute : numeric_attributes) {
    		auto value = row[attribute];
			if(value.is_null()) {
				points->feature_attributes.numeric(attribute).set(i, NAN);
			} else {
				double numericValue;
				try {
					numericValue = value.as<double>();
				} catch (const pqxx::failure&) {
					numericValue = NAN;
				}
				points->feature_attributes.numeric(attribute).set(i, numericValue);
			}
    	}
    	for(auto &attribute : textual_attributes) {
			auto value = row[attribute];
			if (value.is_null()) {
				points->feature_attributes.textual(attribute).set(i, "");
			} else {
				points->feature_attributes.textual(attribute).set(i, value.as<std::string>());
			}
		}
    }
    //points->addDefaultTimestamps();

    return points;
}


std::unique_ptr<PolygonCollection> GFBioSourceOperator::getPolygonCollection(const QueryRectangle &rect, const QueryTools &tools) {
	//connect
	//TODO: reuse
	pqxx::connection connection (Configuration::get("operators.gfbiosource.dbcredentials"));

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
