#include "datatypes/pointcollection.h"
#include "datatypes/geometry.h"

#include "operators/operator.h"
#include "raster/exceptions.h"
#include "util/curl.h"
#include "util/csvparser.h"
#include "util/make_unique.h"

#include <string>
#include <sstream>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/io/WKBReader.h>
#include <json/json.h>


class GFBioPointSourceOperator : public GenericOperator {
	public:
		GFBioPointSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~GFBioPointSourceOperator();

		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<GenericGeometry> getGeometry(const QueryRectangle &rect, QueryProfiler &profiler);
	private:
		void getStringFromServer(const QueryRectangle& rect, std::stringstream& data, std::string format);
		std::string datasource;
		std::string query;
		cURL curl;
		std::string includeMetadata;
};


GFBioPointSourceOperator::GFBioPointSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);

	datasource = params.get("datasource", "").asString();
	query = params.get("query", "").asString();
	params.get("includeMetadata", "false");
	includeMetadata = params.get("includeMetadata", "false").asString();
}

GFBioPointSourceOperator::~GFBioPointSourceOperator() {
}
REGISTER_OPERATOR(GFBioPointSourceOperator, "gfbiopointsource");

class GFBioGeometrySourceOperator : public GFBioPointSourceOperator {
	public:
		GFBioGeometrySourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GFBioPointSourceOperator(sourcecounts, sources, params) {};
};
REGISTER_OPERATOR(GFBioGeometrySourceOperator, "gfbiogeometrysource");



std::unique_ptr<PointCollection> GFBioPointSourceOperator::getPoints(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto points_out = std::make_unique<PointCollection>(rect);

	std::stringstream data;
	getStringFromServer(rect, data, "CSV");
	profiler.addIOCost( data.tellp() );

	try {
		CSVParser parser(data, ',', '\n');

		auto header = parser.readHeaders();
		//TODO: distinguish between double and string properties
		for(int i=2; i < header.size(); i++){
			points_out->local_md_string.addVector(header[i]);
		}

		while(true){
			auto tuple = parser.readTuple();
			if (tuple.size() < 1)
				break;

			size_t idx = points_out->addPoint(std::stod(tuple[0]),std::stod(tuple[1]));
			//double year = std::atof(csv[3].c_str());

			for(int i=2; i < tuple.size(); i++)
				points_out->local_md_string.set(idx, header[i], tuple[i]);
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
std::unique_ptr<GenericGeometry> GFBioPointSourceOperator::getGeometry(const QueryRectangle &rect, QueryProfiler &profiler) {
	if (rect.epsg != EPSG_LATLON) {
		std::ostringstream msg;
		msg << "GFBioSourceOperator: Shouldn't load points in a projection other than latlon (got " << (int) rect.epsg << ", expected " << (int) EPSG_LATLON << ")";
		throw OperatorException(msg.str());
	}

	std::stringstream data;
	getStringFromServer(rect, data, "WKB");
	profiler.addIOCost( data.tellp() );

	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKBReader wkbreader(*gf);

	data.seekg(0);
	geos::geom::Geometry *geom = wkbreader.read(data);

	auto geom_out = std::make_unique<GenericGeometry>(rect);
	geom_out->setGeom(geom);

	return geom_out;
}

void GFBioPointSourceOperator::getStringFromServer(const QueryRectangle& rect, std::stringstream& data, std::string format) {
	std::ostringstream url;
	url
			<< "http://***REMOVED***:81/GFBioJavaWS/Wizzard/fetchDataSource/" << format << "?datasource="
			<< curl.escape(datasource) << "&query=" << curl.escape(query)
			<< "&BBOX=" << std::fixed << rect.x1 << "," << rect.y1 << ","
			<< rect.x2 << "," << rect.y2 << "&includeMetadata=" << includeMetadata;

	//fprintf(stderr, "query: %s\nurl: %s\n", query.c_str(), url.str().c_str());
	//curl.setOpt(CURLOPT_PROXY, "www-cache.mathematik.uni-marburg.de:3128");
	curl.setOpt(CURLOPT_URL, url.str().c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_WRITEDATA, &data);

	curl.perform();
}
