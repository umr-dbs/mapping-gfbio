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


class GFBioSourceOperator : public GenericOperator {
	public:
		GFBioSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~GFBioSourceOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
#ifndef MAPPING_OPERATOR_STUBS
		void getStringFromServer(const QueryRectangle& rect, std::stringstream& data, std::string format);
#endif
		std::string datasource;
		std::string query;
		cURL curl;
		std::string includeMetadata;
};


GFBioSourceOperator::GFBioSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);

	datasource = params.get("datasource", "").asString();
	query = params.get("query", "").asString();
	includeMetadata = params.get("includeMetadata", "false").asString();
}

GFBioSourceOperator::~GFBioSourceOperator() {
}
REGISTER_OPERATOR(GFBioSourceOperator, "gfbio_source");

void GFBioSourceOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "{";
	stream << "\"datasource\":\"" << datasource << "\","
			<< "\"query\":\"" << query << "\","
			<< "\"includeMetadata\":\"" << includeMetadata << "\"";
	stream << "}";
}

#ifndef MAPPING_OPERATOR_STUBS
std::unique_ptr<PointCollection> GFBioSourceOperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto points_out = make_unique<PointCollection>(rect);

	std::stringstream data;
	getStringFromServer(rect, data, "CSV");
	profiler.addIOCost( data.tellp() );

	try {
		CSVParser parser(data, ',');

		auto header = parser.readHeaders();
		//TODO: distinguish between numeric and textual properties, figure out units
		for(int i=2; i < header.size(); i++){
			points_out->feature_attributes.addTextualAttribute(header[i], Unit::unknown());
		}

		while(true){
			auto tuple = parser.readTuple();
			if (tuple.size() < 1)
				break;

			size_t idx = points_out->addSinglePointFeature(Coordinate(std::stod(tuple[0]),std::stod(tuple[1])));
			//double year = std::atof(csv[3].c_str());

			for(int i=2; i < tuple.size(); i++)
				points_out->feature_attributes.textual(header[i]).set(idx, tuple[i]);
		}
		//fprintf(stderr, data.str().c_str());
		return points_out;
	}
	catch (const OperatorException &e) {
		data.seekg(0, std::ios_base::beg);
		fprintf(stderr, "CSV:\n%s\n", data.str().c_str());
		throw;
	}

}


// pc12316:81/GFBioJavaWS/Wizzard/fetchDataSource/WKB?datasource=IUCN&query={"globalAttributes":{"speciesName":"Puma concolor"}}
std::unique_ptr<PolygonCollection> GFBioSourceOperator::getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	if (rect.epsg != EPSG_LATLON) {
		std::ostringstream msg;
		msg << "GFBioSourceOperator: Shouldn't load points in a projection other than latlon (got " << (int) rect.epsg << ", expected " << (int) EPSG_LATLON << ")";
		throw OperatorException(msg.str());
	}

	std::stringstream data;
	getStringFromServer(rect, data, "WKB");
	profiler.addIOCost( data.tellp() );

	auto polygonCollection = WKBUtil::readPolygonCollection(data, rect);

	return polygonCollection;
}

void GFBioSourceOperator::getStringFromServer(const QueryRectangle& rect, std::stringstream& data, std::string format) {
	std::ostringstream url;
	url
		<< Configuration::get("operators.gfbiosource.webserviceurl")
		<< format << "?datasource="
		<< curl.escape(datasource) << "&query=" << curl.escape(query)
		<< "&BBOX=" << std::fixed << rect.x1 << "," << rect.y1 << ","
		<< rect.x2 << "," << rect.y2 << "&includeMetadata=" << includeMetadata;

	curl.setOpt(CURLOPT_PROXY, Configuration::get("operators.gfbiosource.proxy", "").c_str());
	curl.setOpt(CURLOPT_URL, url.str().c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_WRITEDATA, &data);

	curl.perform();
}
#endif
