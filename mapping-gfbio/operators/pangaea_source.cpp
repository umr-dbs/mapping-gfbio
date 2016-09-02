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
	public:
		PangaeaSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual void getProvenance(ProvenanceCollection &pc);

		void parseDataDescription(std::string& dataDescription);
#endif
		void writeSemanticParameters(std::ostringstream& stream) {
			stream << "\"dataLink\":\"" << dataLink;
		}

		virtual ~PangaeaSourceOperator(){};

	private:
		std::string dataLink;
		cURL curl;

		std::string citation;
		std::string license;
		std::string uri;

#ifndef MAPPING_OPERATOR_STUBS
		void getStringFromServer(std::stringstream& data);
#endif
};
REGISTER_OPERATOR(PangaeaSourceOperator, "pangaea_source");


PangaeaSourceOperator::PangaeaSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);
	dataLink = params.get("dataLink", "").asString();


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


std::unique_ptr<PointCollection> PangaeaSourceOperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler){
	std::stringstream data;

	getStringFromServer(data);


	//skip initial comment
	size_t offset = data.str().find("*/\n") + 2;
	std::string dataString = data.str().substr(offset);

	// extract headers
	// TODO: the mapping of the columns should be done on client side
	{
		std::istringstream headerStream(dataString);

		CSVParser csvParser(headerStream, '\t');
		std::vector<std::string> headers = csvParser.readHeaders();

		for(auto header : headers) {
//			fprintf(stderr, header.c_str());
		}
	}

	std::istringstream iss(dataString);

	char field_separator = '\t';

	std::vector<std::string> columns_numeric;
	std::vector<std::string> columns_string;
	CSVSourceUtil csvUtil(GeometrySpecification::XY, TimeSpecification::NONE,
			1.0, "Longitude", "Latitude", "", "", columns_numeric,
			columns_string, field_separator, ErrorHandling::ABORT);

	auto points = csvUtil.getPointCollection(iss, rect);

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
