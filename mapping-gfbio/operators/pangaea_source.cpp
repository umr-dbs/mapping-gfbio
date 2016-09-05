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
#include <boost/regex.hpp>


/**
 * Operator that gets points from pangaea
 *
 * Parameters:
 * - doi: the DOI of the dataset
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
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual void getProvenance(ProvenanceCollection &pc);

		void parseDataDescription(std::string& dataDescription);
		std::vector<Parameter> extractParameters(std::string dataDescription);
#endif
		void writeSemanticParameters(std::ostringstream& stream) {
			stream << "\"dataLink\":\"" << dataLink;
		}

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




#ifndef MAPPING_OPERATOR_STUBS

void PangaeaSourceOperator::parseDataDescription(std::string& dataDescription) {

	boost::regex regex(R"(Citation:\t([^:]+\(\d+\)): ([^\n]+)[.,;] doi:([PANGE0-9.\/]+).*\n.*License:\t([^\n]+)\n)");

	boost::smatch sm;

	if(boost::regex_search (dataDescription, sm, regex)) {
		for (size_t i = 0; i < sm.size(); ++i) {
			fprintf(stderr, ">%s<", sm[i].str().c_str());
		}

		citation = sm[1].str() + ": " + sm[2].str();
		uri = "https://doi.pangaea.de/" + sm[3].str();
		license = sm[4].str();
	}
}

std::vector<PangaeaSourceOperator::Parameter> PangaeaSourceOperator::extractParameters(std::string dataDescription) {
	std::vector<Parameter> parameters;

	// extract parameters part of data description
	boost::regex extractParameters(R"(Parameter\(s\):(.*)License:)");
	boost::smatch sm;

	std::string parametersString;
	if(boost::regex_search (dataDescription, sm, extractParameters)) {
		if(sm.size() > 0)
			parametersString = sm[0].str();
	} else {
		return parameters;
	}

	// extract parameters
	boost::regex regex(R"(\t(([^\(\[]+)(\([^\)]+\))? (\[([^\]]+)])?) ?\(([^\)]+)\)(\n|( \*)))");

	boost::sregex_iterator iter(parametersString.begin(), parametersString.end(), regex);
	boost::sregex_iterator end;

	for (; iter != end; ++iter) {
		auto match = (*iter);
		std::string fullName = match[1];
		std::string name = match[2] + match[3];
		std::string unit = match[5];
		std::string shortName = match[6];

		parameters.push_back(Parameter(fullName, name, unit, shortName));
		fprintf(stderr, "fullName: %s, name: %s, unit: %s, shortName: %s\n", fullName.c_str(), name.c_str(), unit.c_str(), shortName.c_str());
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

std::unique_ptr<PointCollection> PangaeaSourceOperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler){
	std::stringstream data;

	getStringFromServer(data);


	// skip initial comment
	size_t offset = data.str().find("*/\n") + 2;
	std::string dataString = data.str().substr(offset);

	std::string dataDescription = data.str().substr(0, offset);

	std::vector<Parameter> parameters = extractParameters(dataDescription);

	// map parameters to the column name (short name + unit)
	std::vector<std::string> columns_numeric = csvUtil->columns_numeric;
	std::vector<std::string> shortName_numeric;
	for(auto& column : columns_numeric)
		shortName_numeric.push_back(mapParameterNameToColumnName(column, parameters));

	std::vector<std::string> columns_textual = csvUtil->columns_textual;
	std::vector<std::string> shortName_textual;
	for(auto& column : columns_textual)
		shortName_textual.push_back(mapParameterNameToColumnName(column, parameters));

	csvUtil->columns_numeric = shortName_numeric;
	csvUtil->columns_textual = shortName_textual;

	std::string column_x = csvUtil->column_x;
	std::string column_y = csvUtil->column_y;

	if(column_x != "")
		csvUtil->column_x = mapParameterNameToColumnName(column_x, parameters);

	if(column_y != "")
		csvUtil->column_y = mapParameterNameToColumnName(csvUtil->column_y, parameters);


	// parse the .tab file
	std::istringstream iss(dataString);
	auto points = csvUtil->getPointCollection(iss, rect);

	// map column names back to parameters
	for(size_t i = 0; i < columns_numeric.size(); ++i) {
		points->feature_attributes.renameNumericAttribute(csvUtil->columns_numeric[i], columns_numeric[i]);
	}
	for(size_t i = 0; i < columns_textual.size(); ++i) {
		points->feature_attributes.renameTextualAttribute(csvUtil->columns_textual[i], columns_textual[i]);
	}

	// TODO name of geo columns...

	return points;
}

void PangaeaSourceOperator::getStringFromServer(std::stringstream& data) {
//	fprintf(stderr, "url: %s\n", dataLink.c_str());
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
	fprintf(stderr, dataDescription.c_str());

	parseDataDescription(dataDescription);

	pc.add(Provenance(citation, license, uri, "data.pangaea." + dataLink));
}
#endif
