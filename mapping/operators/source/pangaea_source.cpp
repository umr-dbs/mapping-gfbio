#include "operators/operator.h"
#include "datatypes/pointcollection.h"
#include "util/make_unique.h"
#include <sstream>
#include <json/json.h>
#include "util/curl.h"
#include "util/configuration.h"
#include <boost/tokenizer.hpp>
#include <limits>

/**
 * Operator that gets points from pangaea
 */
class PangaeaSourceOperator : public GenericOperator {
	public:
		PangaeaSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
			assumeSources(0);
			doi = params.get("doi", "").asString();
		}

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
		void writeSemanticParameters(std::ostringstream& stream) {
			stream << "\"doi\":\"" << doi;
		}

		virtual ~PangaeaSourceOperator(){};

	private:
		std::string doi;
		cURL curl;

#ifndef MAPPING_OPERATOR_STUBS
		void getStringFromServer(std::stringstream& data);
#endif
};
REGISTER_OPERATOR(PangaeaSourceOperator, "pangaea_source");


#ifndef MAPPING_OPERATOR_STUBS
std::unique_ptr<PointCollection> PangaeaSourceOperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler){
	std::unique_ptr<PointCollection> points = make_unique<PointCollection>(rect);

	std::stringstream data;

	getStringFromServer(data);

	//skip initial comment
	size_t offset = data.str().find("*/\n") + 2;
	std::string dataString = data.str().substr(offset);

	boost::char_delimiters_separator<char> lineSeparator(false, "", "\n"); //dont tokenize based on whitespace or .
	boost::char_delimiters_separator<char> elementSeparator(false, "", "\t");
	boost::tokenizer<boost::char_delimiters_separator<char>> lineTokenizer(dataString, lineSeparator);

	bool atHeader = true;
	size_t longitudeIndex = std::numeric_limits<size_t>::max();
	size_t latitudeIndex = std::numeric_limits<size_t>::max();

	for(auto lineToken = lineTokenizer.begin(); lineToken != lineTokenizer.end(); ++lineToken){
		const std::string& line = *lineToken;
		//std::cout << line << std::endl;
		boost::tokenizer<boost::char_delimiters_separator<char>> elementTokenizer(line, elementSeparator);

		size_t columnIndex = 0;
		Coordinate coordinate(0.0, 0.0);
		//TODO: throw error if no lat/lon present
		for(auto elementToken = elementTokenizer.begin(); elementToken != elementTokenizer.end(); ++elementToken){
			const std::string& element = *elementToken;

			if(atHeader){
				std::string header = element;
				//std::cout << header << std::endl;

				std::transform(header.begin(), header.end(), header.begin(), ::tolower);
				if(header.find("longitude") != std::string::npos){
					longitudeIndex = columnIndex;
				} else if(header.find("latitude") != std::string::npos){
					latitudeIndex = columnIndex;
				} else {
					//TODO handle metadata
				}
			} else {
				if(columnIndex == longitudeIndex){
					coordinate.x = std::stod(element);
				} else if (columnIndex == latitudeIndex){
					coordinate.y = std::stod(element);
				} else {
					//TODO add metadata
				}
			}
			columnIndex++;
		}

		if(atHeader ){
			atHeader = false;
		}
		else {
			points->addSinglePointFeature(coordinate);
		}
	}

	return points;
}

void PangaeaSourceOperator::getStringFromServer(std::stringstream& data) {
	std::ostringstream url;
	url
		<< "http://doi.pangaea.de/"
		<< doi << "?format=textfile";

	curl.setOpt(CURLOPT_PROXY, "www-cache.mathematik.uni-marburg.de:3128");
	curl.setOpt(CURLOPT_URL, url.str().c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_WRITEDATA, &data);

	curl.perform();
}
#endif
