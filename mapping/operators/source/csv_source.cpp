#include "operators/operator.h"
#include "util/exceptions.h"
#include "util/make_unique.h"
#include "util/csv_source_util.h"

#include <string>
#include <fstream>
#include <sstream>
#include <streambuf>
#include <algorithm>
#include <functional>
#include <json/json.h>
#include <sys/stat.h>
#include <memory>

enum class FileType {
	CSV,
	TTX
};

/*
 * Operator that reads files with values delimited by a given value.
 * It conforms to RFC 4180 but adds support for other delimiters.
 * One line in the file corrsponds to one feature.
 *
 * Parameters:
 * - filename: path to the input file
 * - field_separator: the delimiter
 * - geometry_specification: the type in the geometry column(s)
 *   - "xy": two columns for the 2 spatial dimensions
 *   - "wkt": a single column containing the feature geometry as well-known-text
 * - time: the type of the time column(s)
 *   - "none": no time information is mapped
 *   - "start": only start information is mapped. duration has to specified in the duration attribute
 *   - "start+end": start and end information is mapped
 *   - "start+duration": start and duration information is mapped
 * -  duration: the duration of the time validity for all features in the file
 * - time1_format: a json object mapping a column to the start time
 *   - format: define the format of the column
 *     - "custom": define a custom format in the attribute "custom_format"
 *     - "seconds": time column is numeric and contains seconds as UNIX timestamp
 *     - "dmyhm": %d-%B-%Y  %H:%M
 *     - "iso": time column contains string with ISO8601
 * - time2_format: a json object mapping a columns to the end time (cf. time1_format)
 * - columns: a json object mapping the columns to data, time, space. Columns that are not listed are skipped when parsin.
 *   - x: the name of the column containing the x coordinate (or the wkt string)
 *   - y: the name of the column containing the y coordinate
 *   - time1: the name of the first time column
 *   - time2: the name of the second time column
 *   - numeric: an array of column names containing numeric values
 *   - textual: an array of column names containing alpha-numeric values
 * - on_error: specify the type of error handling
 *   - "skip"
 *   - "abort"
 *   - "keep"
 * - provenance: specify the provenance of a file as an array of json object containing
 *   - citation
 *   - license
 *   - uri
 *
 */
class CSVSourceOperator : public GenericOperator {
	public:
		CSVSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~CSVSourceOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, const QueryTools &tools);
		virtual std::unique_ptr<LineCollection> getLineCollection(const QueryRectangle &rect, const QueryTools &tools);
		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, const QueryTools &tools);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);
		virtual void getProvenance(ProvenanceCollection &pc);

	private:
		std::string filename;
		uint64_t filesize;
		FileType filetype;
		Provenance provenance;

		std::unique_ptr<CSVSourceUtil> csvSourceUtil;
};

static uint64_t getFilesize(const char *filename) {
    struct stat st;
    if (stat(filename, &st) == 0)
        return st.st_size;
    return -1;
}

static bool endsWith(const std::string &str, const std::string &suffix) {
	if (str.length() < suffix.length())
		return false;

	return (str.compare(str.length() - suffix.length(), suffix.length(), suffix) == 0);
}

CSVSourceOperator::CSVSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);

	filename = params.get("filename", "").asString();

	// guess separator from file extension
	if (endsWith(filename, ".ttx"))
		filetype = FileType::TTX;
	else
		filetype = FileType::CSV;

	std::string default_separator = (filetype == FileType::TTX ? "\t" : ",");
	auto configured_separator =	params.get("separator", default_separator).asString();

	params["separator"] = configured_separator;

	csvSourceUtil = make_unique<CSVSourceUtil>(params);

	Json::Value provenanceInfo = params["provenance"];
	if (provenanceInfo.isObject()) {
		provenance = Provenance(provenanceInfo.get("citation", "").asString(),
				provenanceInfo.get("license", "").asString(),
				provenanceInfo.get("uri", "").asString(), "");
	}
}


CSVSourceOperator::~CSVSourceOperator() {
}
REGISTER_OPERATOR(CSVSourceOperator, "csv_source");

void CSVSourceOperator::writeSemanticParameters(std::ostringstream& stream) {
	Json::Value params = csvSourceUtil->getParameters();

	params["filename"] = filename;

	Json::Value provenanceInfo;
	provenanceInfo["citation"] = provenance.citation;
	provenanceInfo["license"] = provenance.license;
	provenanceInfo["uri"] = provenance.uri;
	params["provenance"] = provenanceInfo;

	Json::FastWriter writer;
	stream << writer.write(params);
}

#ifndef MAPPING_OPERATOR_STUBS

void CSVSourceOperator::getProvenance(ProvenanceCollection &pc) {
	provenance.local_identifier = "data." + getType() + "." + filename;

	pc.add(provenance);
}

std::unique_ptr<PointCollection> CSVSourceOperator::getPointCollection(const QueryRectangle &rect, const QueryTools &tools) {
	filesize = getFilesize(filename.c_str());
	tools.profiler.addIOCost(filesize);

	std::ifstream data(filename);
	return csvSourceUtil->getPointCollection(data, rect);
}

std::unique_ptr<LineCollection> CSVSourceOperator::getLineCollection(const QueryRectangle &rect, const QueryTools &tools) {
	filesize = getFilesize(filename.c_str());
	tools.profiler.addIOCost(filesize);

	std::ifstream data(filename);
	return csvSourceUtil->getLineCollection(data, rect);
}

std::unique_ptr<PolygonCollection> CSVSourceOperator::getPolygonCollection(const QueryRectangle &rect, const QueryTools &tools) {
	filesize = getFilesize(filename.c_str());
	tools.profiler.addIOCost(filesize);

	std::ifstream data(filename);
	return csvSourceUtil->getPolygonCollection(data, rect);
}


#endif
