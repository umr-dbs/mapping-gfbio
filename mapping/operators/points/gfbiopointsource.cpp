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
		std::string datasource;
		std::string query;
		cURL curl;
};


GFBioPointSourceOperator::GFBioPointSourceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(0);

	datasource = params.get("datasource", "").asString();
	query = params.get("query", "").asString();
}

GFBioPointSourceOperator::~GFBioPointSourceOperator() {
}
REGISTER_OPERATOR(GFBioPointSourceOperator, "gfbiopointsource");

class GFBioGeometrySourceOperator : public GFBioPointSourceOperator {
	public:
		GFBioGeometrySourceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params) : GFBioPointSourceOperator(sourcecount, sources, params) {};
};
REGISTER_OPERATOR(GFBioGeometrySourceOperator, "gfbiogeometrysource");


std::unique_ptr<PointCollection> GFBioPointSourceOperator::getPoints(const QueryRectangle &rect) {
	auto points = loadGeometryFromServer(rect);

	if (points->getDimension() != 0)
		throw OperatorException("Result from GBif is not a point collection");

	auto points_out = std::make_unique<PointCollection>(EPSG_LATLON);
	geos::geom::CoordinateSequence *coords = points->getCoordinates();

	size_t count = coords->getSize();
	for (size_t i=0;i<count;i++) {
		const geos::geom::Coordinate &coord = coords->getAt(i); //[i];
		points_out->addPoint(coord.x, coord.y);
	}
	delete coords; // TODO: is this correct?

	return points_out;
}


// pc12316:81/GFBioJavaWS/Wizzard/fetchDataSource/WKB?datasource=IUCN&query={"globalAttributes":{"speciesName":"Puma concolor"}}
std::unique_ptr<GenericGeometry> GFBioPointSourceOperator::getGeometry(const QueryRectangle &rect) {
	auto geom = loadGeometryFromServer(rect);

	auto geom_out = std::make_unique<GenericGeometry>(EPSG_LATLON);
	geom_out->setGeom(geom.release());

	return geom_out;
}



std::unique_ptr<geos::geom::Geometry> GFBioPointSourceOperator::loadGeometryFromServer(const QueryRectangle &rect) {
	if (rect.epsg != EPSG_LATLON) {
		std::ostringstream msg;
		msg << "GFBioSourceOperator: Shouldn't load points in a projection other than latlon (got " << rect.epsg << ", expected " << EPSG_LATLON << ")";
		throw OperatorException(msg.str());
	}

	std::ostringstream url;
	//url << "http://dbsvm.mathematik.uni-marburg.de:9833/gfbio-prototype/rest/Wizzard/fetchDataSource?datasource" << datasource << "&query=" << query;

	url << "http://pc12316:81/GFBioJavaWS/Wizzard/fetchDataSource/WKB?datasource=" << curl.escape(datasource)
		<< "&query=" << curl.escape(query)
		<< "&BBOX=" << std::fixed << rect.x1 << "," << rect.y1 << "," << rect.x2 << "," << rect.y2;

	std::stringstream data;

	curl.setOpt(CURLOPT_URL, url.str().c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_WRITEDATA, &data);
	//curl.setOpt(CURLOPT_PROXY, "www-cache.mathematik.uni-marburg.de:3128");

	curl.perform();

	// result should now be in our stringstream

//	printf("Content-type: text/plain\r\n\r\nURL: %s\nResult (%u bytes): '%s'\n", url.str().c_str(), data.str().length(), data.str().c_str());
//	exit(5);

	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKBReader wkbreader(*gf);

	data.seekg(0);
	geos::geom::Geometry *geom = wkbreader.read(data);

	return std::unique_ptr<geos::geom::Geometry>(geom);
}
