#include "raster/pointcollection.h"
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
	private:
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

std::unique_ptr<PointCollection> GFBioPointSourceOperator::getPoints(const QueryRectangle &rect) {

	if (rect.epsg != EPSG_LATLON) {
		std::ostringstream msg;
		msg << "GFBioPointSourceOperator: Shouldn't load points in a projection other than latlon (got " << rect.epsg << ", expected " << EPSG_LATLON;
		throw OperatorException(msg.str());
	}

	std::ostringstream url;
	//url << "http://dbsvm.mathematik.uni-marburg.de:9833/gfbio-prototype/rest/Wizzard/fetchDataSource?datasource" << datasource << "&query=" << query;

	url << "http://pc12316:81/GFBioJavaWS/Wizzard/fetchDataSource/WKB?datasource=" << curl.escape(datasource) << "&query=" << curl.escape(query);

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

	auto points_out = std::make_unique<PointCollection>(EPSG_LATLON);

	data.seekg(0);
	geos::geom::Geometry *points = wkbreader.read(data);

	if (points->getDimension() != 0)
		throw OperatorException("Result from GBif is not a point collection");

	geos::geom::CoordinateSequence *coords = points->getCoordinates();

	size_t count = coords->getSize();
	for (size_t i=0;i<count;i++) {
		const geos::geom::Coordinate &coord = coords->getAt(i); //[i];
		points_out->addPoint(coord.x, coord.y);
	}
	delete coords; // TODO: is this correct?

	gf->destroyGeometry(points);

	return points_out;
}
