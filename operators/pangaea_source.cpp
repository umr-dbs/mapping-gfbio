#include "operators/operator.h"
#include "datatypes/pointcollection.h"

#include "util/make_unique.h"
#include "util/curl.h"
#include "util/configuration.h"
#include "util/csv_source_util.h"
#include "util/timeparser.h"
#include "util/csvparser.h"


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
		virtual void getProvenance(ProvenanceCollection &pc);

		void parseDataDescription(std::string& dataDescription);
		std::vector<Parameter> extractParameters(std::string dataDescription);
#endif
		void writeSemanticParameters(std::ostringstream& stream);

		virtual ~PangaeaSourceOperator(){};

	private:
		std::string dataLink;
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

		std::string mapParameterNameToColumnName(std::string fullName, std::vector<PangaeaSourceOperator::Parameter> parameters);
		std::string mapNameToFullName(std::string shortName, std::vector<PangaeaSourceOperator::Parameter> parameters);
#endif
};
REGISTER_OPERATOR(PangaeaSourceOperator, "pangaea_source");


PangaeaSourceOperator::PangaeaSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);
	dataLink = params.get("dataLink", "").asString();

	csvUtil = make_unique<CSVSourceUtil>(params);
}

void PangaeaSourceOperator::writeSemanticParameters(std::ostringstream& stream) {
	Json::Value params = csvUtil->getParameters();
	params["dataLink"] = dataLink;

	Json::FastWriter writer;
	stream << writer.write(params);
}


#ifndef MAPPING_OPERATOR_STUBS

void PangaeaSourceOperator::parseDataDescription(std::string& dataDescription) {

	std::regex regex(R"(Citation:\t([^:]+\(\d+\)): ([^\n]+)[.,;] doi:([PANGE0-9.\/]+)(?:.|\r|\n)*License:\t([^\n]+)\n)");

	std::smatch sm;

	if(std::regex_search (dataDescription, sm, regex)) {
		citation = sm[1].str() + ": " + sm[2].str();
		uri = "https://doi.pangaea.de/" + sm[3].str();
		license = sm[4].str();
	}
}

std::vector<PangaeaSourceOperator::Parameter> PangaeaSourceOperator::extractParameters(std::string dataDescription) {
	std::vector<Parameter> parameters;

	// extract parameters part of data description
	std::regex extractParameters(R"(Parameter\(s\):((?:.|\r|\n)*)License:)");
	std::smatch sm;

	std::string parametersString;
	if(std::regex_search (dataDescription, sm, extractParameters)) {
		if(sm.size() > 0)
			parametersString = sm[0].str();
	} else {
		return parameters;
	}

	// extract parameters
	std::regex regex(R"(\t(([^\(\[]+)(\([^\)]+\))? (\[([^\]]+)])?) ?\(([^\)]+)\)(\n|( \*)))");

	std::sregex_iterator iter(parametersString.begin(), parametersString.end(), regex);
	std::sregex_iterator end;

	for (; iter != end; ++iter) {
		auto match = (*iter);
		std::string fullName = match[1];
		std::string name = match[2].str() + match[3].str();
		std::string unit = match[5];
		std::string shortName = match[6];

		parameters.push_back(Parameter(fullName, name, unit, shortName));
	}

	return parameters;
}


std::string PangaeaSourceOperator::mapParameterNameToColumnName(std::string name, std::vector<PangaeaSourceOperator::Parameter> parameters) {
	for(auto& parameter : parameters) {
		if(parameter.name == name) {
			std::string columnName = parameter.shortName;
			if(parameter.unit != "")
				columnName += " [" + parameter.unit + "]";
			return columnName;
		}
	}
	throw std::runtime_error("PangaeaSource: invalid parameter name " + name);
}

std::string PangaeaSourceOperator::mapNameToFullName(std::string shortName, std::vector<PangaeaSourceOperator::Parameter> parameters) {
	for(auto& parameter : parameters) {
		if(parameter.shortName == shortName)
			return parameter.name;
	}
	throw std::runtime_error("PangaeaSource: invalid parameter name " + shortName);
}

std::unique_ptr<PointCollection> PangaeaSourceOperator::getPointCollection(const QueryRectangle &rect, const QueryTools &tools){
	std::stringstream data;

	getStringFromServer(data);


	// skip initial comment
	size_t offset = data.str().find("*/\n") + 3;
	std::string dataString = data.str().substr(offset);

	std::string dataDescription = data.str().substr(0, offset);

	std::vector<Parameter> parameters = extractParameters(dataDescription);

	// map parameters to the column name (short name + unit). FAIL: column name can also contain comment
	std::stringstream headerStream(dataString);
	CSVParser csvParser(headerStream, csvUtil->field_separator);
	std::vector<std::string> csvColumns = csvParser.readHeaders();

	std::map<std::string, std::string> columnNameToParameter;

	std::vector<std::string> columns_numeric = csvUtil->columns_numeric;
	std::vector<std::string> shortName_numeric;
	std::vector<std::string> failed_numeric, failed_textual; // requested columns that couldn't be resolved an will be returned empty
	for(auto& column : columns_numeric) {
		try {
			std::string columnName = mapParameterNameToColumnName(column, parameters);
			if(std::find(csvColumns.begin(), csvColumns.end(), columnName) != csvColumns.end()) {
				// mapped column name exists
				shortName_numeric.push_back(columnName);
				columnNameToParameter.emplace(columnName, column);
			} else {
				failed_numeric.push_back(column);
			}
		} catch (...) {
			failed_numeric.push_back(column);
		}
	}
	std::vector<std::string> columns_textual = csvUtil->columns_textual;
	std::vector<std::string> shortName_textual;
	for(auto& column : columns_textual) {
		try {
			std::string columnName = mapParameterNameToColumnName(column, parameters);
			if(std::find(csvColumns.begin(), csvColumns.end(), columnName) != csvColumns.end()) {
				// mapped column name exists
				shortName_textual.push_back(columnName);
				columnNameToParameter.emplace(columnName, column);
			} else {
				failed_textual.push_back(column);
			}
		} catch (...) {
			failed_textual.push_back(column);
		}
	}

	csvUtil->columns_numeric = shortName_numeric;
	csvUtil->columns_textual = shortName_textual;

	std::string column_x = csvUtil->column_x;
	std::string column_y = csvUtil->column_y;

	if(column_x != "")
		csvUtil->column_x = mapParameterNameToColumnName(column_x, parameters);

	if(column_y != "")
		csvUtil->column_y = mapParameterNameToColumnName(column_y, parameters);

	// parse the .tab file
	std::istringstream iss(dataString);
	auto points = csvUtil->getPointCollection(iss, rect);

	AttributeArrays mapped_attributes;

	// map column names back to parameters
	for(size_t i = 0; i < csvUtil->columns_numeric.size(); ++i) {
		std::string &column = csvUtil->columns_numeric[i];
		std::string &parameter = columnNameToParameter[column];
		auto &attribute = mapped_attributes.addNumericAttribute(parameter, points->feature_attributes.numeric(column).unit);
		attribute = std::move(points->feature_attributes.numeric(column));
	}
	for(size_t i = 0; i < csvUtil->columns_textual.size(); ++i) {
		std::string &column = csvUtil->columns_textual[i];
		std::string &parameter = columnNameToParameter[column];
		auto &attribute = mapped_attributes.addTextualAttribute(parameter, points->feature_attributes.textual(column).unit);
		attribute = std::move(points->feature_attributes.textual(column));
	}

	points->feature_attributes = std::move(mapped_attributes);

	// failed columns
	for(std::string &column : failed_numeric) {
		points->feature_attributes.addNumericAttribute(column, Unit::unknown());
		points->feature_attributes.numeric(column).resize(points->getFeatureCount());
	}
	for(std::string &column : failed_textual) {
		points->feature_attributes.addTextualAttribute(column, Unit::unknown());
		points->feature_attributes.textual(column).resize(points->getFeatureCount());
	}

	// TODO name of geo columns...

	return points;
}

void PangaeaSourceOperator::getStringFromServer(std::stringstream& data) {
	curl.setOpt(CURLOPT_PROXY, Configuration::get("proxy", "").c_str());
	curl.setOpt(CURLOPT_URL, dataLink.c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_WRITEDATA, &data);

	curl.perform();
}

void PangaeaSourceOperator::getProvenance(ProvenanceCollection &pc) {
	// TODO avoid loading ALL data again
	std::stringstream data;

	getStringFromServer(data);

	// skip initial comment
	size_t offset = data.str().find("*/\n") + 2;
	std::string dataDescription = data.str().substr(0, offset);

	parseDataDescription(dataDescription);

	pc.add(Provenance(citation, license, uri, "data.pangaea_source"));
}
#endif
