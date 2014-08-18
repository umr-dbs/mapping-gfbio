#include "raster/pointcollection.h"
#include "raster/geometry.h"

#include "operators/operator.h"
#include "util/curl.h"
#include "util/make_unique.h"

#include <string>
#include <sstream>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/io/WKBReader.h>
#include <json/json.h>


class GFBioPointSourceOperator : public GenericOperator {
	public:
		GFBioPointSourceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~GFBioPointSourceOperator();

		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
		virtual std::unique_ptr<GenericGeometry> getGeometry(const QueryRectangle &rect);
	private:
		std::unique_ptr<geos::geom::Geometry> loadGeometryFromServer(const QueryRectangle &qrect);
		void getStringFromServer(const QueryRectangle& rect, std::stringstream& data, std::string format);
		std::string datasource;
		std::string query;
		cURL curl;
		std::vector<std::string> parseCSVLine(std::string line);
		std::string includeMetadata;
};


GFBioPointSourceOperator::GFBioPointSourceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(0);

	datasource = params.get("datasource", "").asString();
	query = params.get("query", "").asString();
	params.get("includeMetadata", "false");
	includeMetadata = params.get("includeMetadata", "false").asString();
	//fprintf(stderr, params.get("includeMetadata", "hahahaha").asCString());
}

GFBioPointSourceOperator::~GFBioPointSourceOperator() {
}
REGISTER_OPERATOR(GFBioPointSourceOperator, "gfbiopointsource");

class GFBioGeometrySourceOperator : public GFBioPointSourceOperator {
	public:
		GFBioGeometrySourceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params) : GFBioPointSourceOperator(sourcecount, sources, params) {};
};
REGISTER_OPERATOR(GFBioGeometrySourceOperator, "gfbiogeometrysource");

std::vector<std::string> GFBioPointSourceOperator::parseCSVLine(std::string line){
	//TODO: use array
	std::vector<std::string> csv;

	//parse csv line, can't handle embedded quotes
	int start = 0;
	bool inQuote = false;
	char separator = ',';
	char quote = '\"';
	for (int i = 0 ; i <= line.length(); i++)  {
		if(line[i] == separator || i == line.length() ){
			if(!inQuote){
				//token goes from start to i
				csv.push_back(line.substr(start, i-start));

				start = i+1;
			}
		} else if(line[i] == quote){
			inQuote = !inQuote;
		}
	}

	return csv;
}

std::unique_ptr<PointCollection> GFBioPointSourceOperator::getPoints(const QueryRectangle &rect) {
	auto points_out = std::make_unique<PointCollection>(EPSG_LATLON);

	std::stringstream data;
	getStringFromServer(rect, data, "CSV");

	std::string line;

	//header
	std::getline(data,line);
	auto header = parseCSVLine(line);
	//TODO: distinguish between double and string properties
	for(int i=2; i < header.size(); i++){
		points_out->addLocalMDString(header[i]);
	}

	while(std::getline(data,line)){
			auto csv = parseCSVLine(line);

			Point& point = points_out->addPoint(std::stod(csv[0]),std::stod(csv[1]));
			//double year = std::atof(csv[3].c_str());

			for(int i=2; i < csv.size(); i++){
				points_out->setLocalMDString(point, header[i], csv[i]);
			}
	}
	//fprintf(stderr, data.str().c_str());
	return points_out;
}


// pc12316:81/GFBioJavaWS/Wizzard/fetchDataSource/WKB?datasource=IUCN&query={"globalAttributes":{"speciesName":"Puma concolor"}}
std::unique_ptr<GenericGeometry> GFBioPointSourceOperator::getGeometry(const QueryRectangle &rect) {
	auto geom = loadGeometryFromServer(rect);

	auto geom_out = std::make_unique<GenericGeometry>(EPSG_LATLON);
	geom_out->setGeom(geom.release());

	return geom_out;
}

void GFBioPointSourceOperator::getStringFromServer(const QueryRectangle& rect, std::stringstream& data, std::string format) {
	std::ostringstream url;
	url
			<< "http://pc12316:81/GFBioJavaWS/Wizzard/fetchDataSource/" << format << "?datasource="
			<< curl.escape(datasource) << "&query=" << curl.escape(query)
			<< "&BBOX=" << std::fixed << rect.x1 << "," << rect.y1 << ","
			<< rect.x2 << "," << rect.y2 << "&includeMetadata=" << includeMetadata;

	fprintf(stderr, query.c_str());
	curl.setOpt(CURLOPT_URL, url.str().c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_WRITEDATA, &data);

	curl.perform();
}

std::unique_ptr<geos::geom::Geometry> GFBioPointSourceOperator::loadGeometryFromServer(const QueryRectangle &rect) {
	if (rect.epsg != EPSG_LATLON) {
		std::ostringstream msg;
		msg << "GFBioSourceOperator: Shouldn't load points in a projection other than latlon (got " << rect.epsg << ", expected " << EPSG_LATLON << ")";
		throw OperatorException(msg.str());
	}


	//url << "http://dbsvm.mathematik.uni-marburg.de:9833/gfbio-prototype/rest/Wizzard/fetchDataSource?datasource" << datasource << "&query=" << query;
	std::stringstream data;
	
	getStringFromServer(rect, data, "WKB");
	//curl.setOpt(CURLOPT_PROXY, "www-cache.mathematik.uni-marburg.de:3128");

	// result should now be in our stringstream

//	printf("Content-type: text/plain\r\n\r\nURL: %s\nResult (%u bytes): '%s'\n", url.str().c_str(), data.str().length(), data.str().c_str());
//	exit(5);

	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKBReader wkbreader(*gf);

	data.seekg(0);
	geos::geom::Geometry *geom = wkbreader.read(data);

	return std::unique_ptr<geos::geom::Geometry>(geom);
}
