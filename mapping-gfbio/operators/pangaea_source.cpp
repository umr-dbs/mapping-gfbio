#include "operators/operator.h"
#include "datatypes/pointcollection.h"

#include "util/make_unique.h"
#include "util/curl.h"
#include "util/configuration.h"
#include "util/csv_source_util.h"
#include "util/timeparser.h"


#include <vector>
#include <limits>
#include <sstream>
#include <json/json.h>
/**
 * Operator that gets points from pangaea
 *
 * Parameters:
 * - doi: the DOI of the dataset
 */
class PangaeaSourceOperator : public GenericOperator {
	public:
		PangaeaSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
			assumeSources(0);
			dataLink = params.get("dataLink", "").asString();
		}

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
		void writeSemanticParameters(std::ostringstream& stream) {
			stream << "\"dataLink\":\"" << dataLink;
		}

		virtual ~PangaeaSourceOperator(){};

	private:
		std::string dataLink;
		cURL curl;

#ifndef MAPPING_OPERATOR_STUBS
		void getStringFromServer(std::stringstream& data);
#endif
};
REGISTER_OPERATOR(PangaeaSourceOperator, "pangaea_source");


#ifndef MAPPING_OPERATOR_STUBS
std::unique_ptr<PointCollection> PangaeaSourceOperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler){
	std::stringstream data;

	getStringFromServer(data);
	//fprintf(stderr, "%s\n", data.str().c_str());
	//skip initial comment
	// TODO: parse comment or retrieve metadata info in another way and extract parameters

	size_t offset = data.str().find("*/\n") + 2;
	std::string dataString = data.str().substr(offset);

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
	fprintf(stderr, "url: %s\n", dataLink.c_str());
	curl.setOpt(CURLOPT_PROXY, Configuration::get("proxy", "").c_str());
	curl.setOpt(CURLOPT_URL, dataLink.c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_WRITEDATA, &data);

	curl.perform();
}
#endif
