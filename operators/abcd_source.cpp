#include "operators/operator.h"
#include "datatypes/pointcollection.h"
#include "util/make_unique.h"
#include "util/exceptions.h"
#include "util/configuration.h"
#include "util/stringsplit.h"

#include <sstream>
#include <json/json.h>
#include <algorithm>
#include <string>
#include <functional>
#include <memory>
#include <cctype>
#include <unordered_set>
#include <vector>
#include <pugixml.hpp>
#include <iostream>



/**
 * Operator that reads a given ABCD file and loads all units
 *
 * Parameters:
 * - archive: the path of the ABCD file
 * - units: an array with unit identifiers that specifies the units that are returned (optional)
 * - columns:
 * 		- numeric: array of column names of numeric type, XML path relative to DataSets/DataSet/Units/Unit
 * 		- textual: array of column names of textual type, XML path relative to DataSets/DataSet/Units/Unit
 */
class ABCDSourceOperator : public GenericOperator {
	public:
		ABCDSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, const QueryTools &tools);
		virtual void getProvenance(ProvenanceCollection &pc);
#endif
		void writeSemanticParameters(std::ostringstream& stream);

		virtual ~ABCDSourceOperator(){};

	private:
		std::string archive;
		std::string inputFile;

		bool filterUnitsById = false;
		std::unordered_set<std::string> unitIds;

#ifndef MAPPING_OPERATOR_STUBS
		std::string abcdPrefix;

		std::vector<std::string> numeric_attributes;
		std::vector<std::string> textual_attributes;

		std::map<std::string, std::string> xpathQueries;

		std::string getXPathQuery(std::string &attribute);

		std::string prefix(const char *name);

		std::unique_ptr<PointCollection> createFeatureCollectionWithAttributes(const QueryRectangle &rect);
#endif

};
REGISTER_OPERATOR(ABCDSourceOperator, "abcd_source");



ABCDSourceOperator::ABCDSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);
	archive = params.get("path", "").asString();

	// map archive url to local file
	std::stringstream ss;
	for(char& c : archive) {
		if (isalnum(c))
			ss << c;
		else
			ss << '_';
	}
	ss << ".xml";

	inputFile = ss.str();

	// filters on unitId
	if (params.isMember("units") && params["units"].size() > 0) {
		filterUnitsById = true;
		for (Json::Value &unit : params["units"]) {
			unitIds.emplace(unit.asString());
		}
	}

	// attributes to be extracted
	if(!params.isMember("columns") || !params["columns"].isObject())
		throw ArgumentException("ABCDSourceOperator: columns are not specified");

	auto columns = params["columns"];
	if(!columns.isMember("numeric") || !columns["numeric"].isArray())
		throw ArgumentException("ABCDSourceOperator: numeric columns are not specified");

	if(!columns.isMember("textual") || !columns["textual"].isArray())
		throw ArgumentException("ABCDSourceOperator: textual columns are not specified");

	for(auto &attribute : columns["numeric"])
		numeric_attributes.push_back(attribute.asString());

	for(auto &attribute : columns["textual"])
		textual_attributes.push_back(attribute.asString());
}

void ABCDSourceOperator::writeSemanticParameters(std::ostringstream& stream) {
	Json::Value json(Json::objectValue);
	json["path"] = archive;

	// TODO: sort values to avoid unnecessary cache misses

	Json::Value jsonUnits(Json::arrayValue);
	for(auto &unit : unitIds)
		jsonUnits.append(unit);
	json["units"] = jsonUnits;


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

std::unique_ptr<PointCollection> ABCDSourceOperator::createFeatureCollectionWithAttributes(const QueryRectangle &rect) {
	auto points = make_unique<PointCollection>(rect);

	auto unit_textual_attributes = split(Configuration::get("gfbio.abcd.textualattributes", ""), ' ');

	for(auto& attribute : numeric_attributes) {
		points->feature_attributes.addNumericAttribute(attribute, Unit::unknown());
	}

	for(auto& attribute : textual_attributes) {
		points->feature_attributes.addTextualAttribute(attribute, Unit::unknown());
	}

	return points;
}

std::string ABCDSourceOperator::prefix(const char* name) {
	return abcdPrefix + ":" + name;
}

std::string ABCDSourceOperator::getXPathQuery(std::string &attribute) {
	if(xpathQueries.count(attribute) == 1) {
		return xpathQueries[attribute];
	} else {
		std::stringstream result;
		std::vector<std::string> splitted = split(attribute, '/');

		for(size_t i = 0; i < splitted.size(); ++i) {
			if(i > 0) {
				result << "/";
			}
			result << abcdPrefix << ":" << splitted[i];
		}

		std::string query = result.str();
		xpathQueries[attribute] = query;

		return query;
	}
}

std::unique_ptr<PointCollection> ABCDSourceOperator::getPointCollection(const QueryRectangle &rect, const QueryTools &tools){
	// TODO: global attributes

	pugi::xml_document doc;

	std::string filePath = Configuration::get("gfbio.abcd.datapath") + "/" +inputFile;
	pugi::xml_parse_result result = doc.load_file(filePath.c_str());

	if(!result) {
		throw OperatorException("ABCDSouce: Could not load file with given name");
	}

	abcdPrefix = doc.first_child().name();
	abcdPrefix = abcdPrefix.substr(0, abcdPrefix.find(":"));

	pugi::xml_node dataSet = doc.child(prefix("DataSets").c_str()).child(prefix("DataSet").c_str());

	if(dataSet.empty()) {
		throw OperatorException("ABCDSource: DataSet not found in XML");
	}

	auto points = createFeatureCollectionWithAttributes(rect);

	pugi::xml_node units = dataSet.child(prefix("Units").c_str());

	for (pugi::xml_node unit = units.child(prefix("Unit").c_str()); unit; unit = unit.next_sibling(prefix("Unit").c_str())) {
		if(filterUnitsById) {
			std::string guid = unit.child(prefix("UnitID").c_str()).text().get();

			if(unitIds.count(guid) == 0) {
				continue;
			}
		}

		// coordinates
		auto gathering = unit.child(prefix("Gathering").c_str());
		auto coordinates = gathering.child(prefix("SiteCoordinateSets").c_str()).child(prefix("SiteCoordinates").c_str()).child(prefix("CoordinatesLatLong").c_str());

		if (!coordinates.empty()) {
			double x = coordinates.child(prefix("LongitudeDecimal").c_str()).text().as_double(0);
			double y = coordinates.child(prefix("LatitudeDecimal").c_str()).text().as_double(0);

			points->addSinglePointFeature(Coordinate(x,y));
		} else {
			continue;
		}


		// attributes
		for(auto& attribute : points->feature_attributes.getNumericKeys()) {
			std::string query = getXPathQuery(attribute);
			double value = unit.select_node(query.c_str()).node().text().as_double(NAN);
			points->feature_attributes.numeric(attribute).set(points->getFeatureCount() - 1, value);
		}

		for(auto& attribute : points->feature_attributes.getTextualKeys()) {
			std::string query = getXPathQuery(attribute);
			std::string value = unit.select_node(query.c_str()).node().text().get();
			points->feature_attributes.textual(attribute).set(points->getFeatureCount() - 1, value);
		}

	}

	return points->filterBySpatioTemporalReferenceIntersection(rect);
}


void ABCDSourceOperator::getProvenance(ProvenanceCollection &pc) {
	pugi::xml_document doc;

	std::string filePath = Configuration::get("gfbio.abcd.datapath") + "/" +inputFile;
	pugi::xml_parse_result result = doc.load_file(filePath.c_str());

	if(!result) {
		throw OperatorException("ABCDSouce: Could not load file with given name");
	}

	abcdPrefix = doc.first_child().name();
	abcdPrefix = abcdPrefix.substr(0, abcdPrefix.find(":"));

	Provenance provenance;
	provenance.local_identifier = "data." + getType();

	std::string citationPath = "DataSets/DataSet/Metadata/IPRStatements/Citations/Citation/Text";
	std::string uriPath = "DataSets/DataSet/Metadata/Description/Representation/URI";
	std::string licensePath = "DataSets/DataSet/Metadata/IPRStatements/Licenses/License/Text";

	provenance.citation = doc.select_node(getXPathQuery(citationPath).c_str()).node().text().get();
	provenance.uri += doc.select_node(getXPathQuery(uriPath).c_str()).node().text().get();
	provenance.license += doc.select_node(getXPathQuery(licensePath).c_str()).node().text().get();

	pc.add(provenance);
}

#endif
