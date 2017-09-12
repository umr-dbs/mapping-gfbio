#include "operators/operator.h"
#include "datatypes/pointcollection.h"

#include "util/make_unique.h"
#include "util/curl.h"
#include "util/configuration.h"
#include "util/csv_source_util.h"
#include "util/timeparser.h"
#include "util/csvparser.h"
#include "util/pangaeaapi.h"


#include <vector>
#include <limits>
#include <sstream>
#include <iostream>
#include <json/json.h>
#include <regex>


/**
 * Operator that gets points from pangaea
 *
 * Parameters:
 * - dataLink: the link to the tab separated data file
 * - other csv columns
 */
class PangaeaSourceOperator : public GenericOperator {
	private:
		class Parameter {
					public:
						Parameter(const std::string &fullName, const std::string &name,
								const std::string &unit, const std::string &shortName) :
								fullName(fullName), name(name), unit(unit), shortName(shortName) {
						}

						virtual ~Parameter() {};

						std::string fullName;
						std::string name;
						std::string unit;
						std::string shortName;
				};

	public:
		PangaeaSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, const QueryTools &tools);
		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, const QueryTools &tools);
		virtual void getProvenance(ProvenanceCollection &pc);

		void parseDataDescription(std::string& dataDescription);
		std::vector<Parameter> extractParameters(std::string dataDescription);
#endif
		void writeSemanticParameters(std::ostringstream& stream);

		virtual ~PangaeaSourceOperator(){};

	private:
		std::string doi;
		cURL curl;

		std::vector<std::string> columns_textual;
		std::vector<std::string> columns_numeric;

		std::string column_x;
		std::string column_y;

		std::string citation;
		std::string license;
		std::string uri;

		std::unique_ptr<CSVSourceUtil> csvUtil;


#ifndef MAPPING_OPERATOR_STUBS
		void getStringFromServer(std::stringstream& data);

		/**
		 * check if lat/lon parameters exist
		 * */
		bool hasGeoReference(const std::vector<PangaeaAPI::Parameter> parameters);

		std::string buildCSVHeader(const std::vector<PangaeaAPI::Parameter> parameters);

		std::string extractCSV(std::stringstream &data, PangaeaAPI::MetaData &metaData);
#endif
};
REGISTER_OPERATOR(PangaeaSourceOperator, "pangaea_source");


PangaeaSourceOperator::PangaeaSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);
	doi = params.get("doi", "").asString();

	csvUtil = make_unique<CSVSourceUtil>(params);
}

void PangaeaSourceOperator::writeSemanticParameters(std::ostringstream& stream) {
	Json::Value params = csvUtil->getParameters();
	params["doi"] = doi;

	Json::FastWriter writer;
	stream << writer.write(params);
}


#ifndef MAPPING_OPERATOR_STUBS

bool PangaeaSourceOperator::hasGeoReference(const std::vector<PangaeaAPI::Parameter> parameters) {
	bool hasLat = false;
	bool hasLon = false;

	for(auto& parameter: parameters) {
		if(parameter.name == "Latitude" || parameter.name == "LATITUDE") {
			hasLat = true;
		} else if (parameter.name == "Longitude" || parameter.name == "LONGITUDE") {
			hasLon = true;
		}
	}

	return hasLat && hasLon;
}

std::string PangaeaSourceOperator::buildCSVHeader(const std::vector<PangaeaAPI::Parameter> parameters) {
	std::stringstream ss;

	for(size_t i = 0; i < parameters.size(); ++i) {
		if(i > 0) {
			ss << csvUtil->field_separator;
		}
		ss << "\"" << parameters[i].name << "\"";
	}

	ss << "\n";

	return ss.str();
}

std::string PangaeaSourceOperator::extractCSV(std::stringstream &data, PangaeaAPI::MetaData &metaData) {
	// skip initial comment
	std::string str = data.str();
	size_t offset = str.find("*/\n") + 3;
	// skip header column
	// TODO handle \n in column headers
	offset = str.find("\n", offset) + 1;

	std::string dataString = buildCSVHeader(metaData.parameters);
	dataString += data.str().substr(offset);

	return dataString;
}

std::unique_ptr<PointCollection> PangaeaSourceOperator::getPointCollection(const QueryRectangle &rect, const QueryTools &tools){
	PangaeaAPI::MetaData metaData = PangaeaAPI::getMetaData(doi);

	std::stringstream data;
	getStringFromServer(data);

	std::string dataString = extractCSV(data, metaData);

	if(!hasGeoReference(metaData.parameters)) {
		csvUtil->default_x = metaData.spatialCoverageWKT;
	}

	std::istringstream iss(dataString);
	auto points = csvUtil->getPointCollection(iss, rect);

	return points;
}

std::unique_ptr<PolygonCollection> PangaeaSourceOperator::getPolygonCollection(const QueryRectangle &rect, const QueryTools &tools){
	PangaeaAPI::MetaData metaData = PangaeaAPI::getMetaData(doi);

	std::stringstream data;
	getStringFromServer(data);

	std::string dataString = extractCSV(data, metaData);

	if(!hasGeoReference(metaData.parameters)) {
		csvUtil->default_x = metaData.spatialCoverageWKT;
		fprintf(stderr, ">> %s", metaData.spatialCoverageWKT.c_str());
	}

	std::istringstream iss(dataString);
	auto polygons = csvUtil->getPolygonCollection(iss, rect);

	return polygons;
}

void PangaeaSourceOperator::getStringFromServer(std::stringstream& data) {
	curl.setOpt(CURLOPT_PROXY, Configuration::get("proxy", "").c_str());
	curl.setOpt(CURLOPT_URL, concat("https://doi.pangaea.de/", doi, "?format=textfile").c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_WRITEDATA, &data);

	curl.perform();
}

void PangaeaSourceOperator::getProvenance(ProvenanceCollection &pc) {
	std::stringstream data;

	Provenance provenance;

	provenance.citation = PangaeaAPI::getCitation(doi);

	PangaeaAPI::MetaData metaData = PangaeaAPI::getMetaData(doi);

	provenance.license = metaData.license;
	provenance.uri = metaData.url;

	provenance.local_identifier =  "data." + getType();

	pc.add(provenance);
}
#endif
