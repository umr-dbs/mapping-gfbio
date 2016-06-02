#include "services/ogcservice.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "operators/operator.h"
#include "pointvisualization/CircleClusteringQuadTree.h"
#include "util/timeparser.h"
#include "util/enumconverter.h"

#include <string>
#include <cmath>
#include <map>
#include <sstream>
#include <json/json.h>
#include <utility>
#include <vector>

enum class WFSServiceType {
	GetCapabilities, GetFeature
};

enum class FeatureType {
	POINTS, LINES, POLYGONS
};

const std::vector< std::pair<FeatureType, std::string> > featureTypeMap = {
	std::make_pair(FeatureType::POINTS, "points"),
	std::make_pair(FeatureType::LINES, "lines"),
	std::make_pair(FeatureType::POLYGONS, "polygons"),
};

EnumConverter<FeatureType> featureTypeConverter(featureTypeMap);

/**
 * A simple incomplete implementation of the WFS standard, just enough so that OpenLayers can use it.
 */
class WFSService : public OGCService {
	public:
		using OGCService::OGCService;
		virtual ~WFSService() = default;

		std::string getResponse(const Params &params);

		virtual void run();

	private:
		std::string getCapabilities();
		void getFeature();

		// TODO: implement
		std::string describeFeatureType();
		std::string getPropertyValue();
		std::string listStoredQueries();
		std::string describeStoredQueries();

		// helper functions
		std::pair<FeatureType, Json::Value> parseTypeNames(const std::string &typeNames) const;
		std::unique_ptr<PointCollection> clusterPoints(const PointCollection &points, const Params &params) const;

		const std::map<std::string, WFSServiceType> stringToRequest {
			{"GetCapabilities", WFSServiceType::GetCapabilities},
			{"GetFeature", WFSServiceType::GetFeature}
		};


};
REGISTER_HTTP_SERVICE(WFSService, "WFS");


void WFSService::run() {
	if (!params.hasParam("version") || params.get("version") != "2.0.0") {
		response.send500("wrong version");
	}

	switch (stringToRequest.at(params.get("request"))) {
	case WFSServiceType::GetCapabilities:
		getCapabilities();

		break;
	case WFSServiceType::GetFeature:
		getFeature();

		break;
	default:
		response.send500("wrong request");
		break;
	}
}

std::string WFSService::getCapabilities() {
	response.sendContentType("text/html");
	response.finishHeaders();
}

void WFSService::getFeature() {
	if(!params.hasParam("typenames"))
		throw ArgumentException("WFSService: typeNames parameter not specified");

	std::pair<FeatureType, Json::Value> typeNames = parseTypeNames(params.get("typenames"));
	FeatureType featureType = typeNames.first;
	Json::Value query = typeNames.second;

	TemporalReference tref = parseTime(params);

	// srsName=CRS
	// this parameter is optional in WFS, but we use it here to create the Spatial Reference.
	if(!params.hasParam("srsname"))
		throw new ArgumentException("WFSService: Parameter srsname is missing");
	epsg_t queryEpsg = this->parseEPSG(params, "srsname");


	SpatialReference sref(queryEpsg);
	if(params.hasParam("bbox")) {
		sref = parseBBOX(params.get("bbox"), queryEpsg);
	}

	auto graph = GenericOperator::fromJSON(query);

	QueryProfiler profiler;

	QueryRectangle rect(
		sref,
		tref,
		QueryResolution::none()
	);

	std::unique_ptr<SimpleFeatureCollection> features;

	switch (featureType){
	case FeatureType::POINTS:
		features = graph->getCachedPointCollection(rect, profiler);
		break;
	case FeatureType::LINES:
		features = graph->getCachedLineCollection(rect, profiler);
		break;
	case FeatureType::POLYGONS:
		features = graph->getCachedPolygonCollection(rect, profiler);
		break;
	}

	//clustered is ignored for non-point collections
	//TODO: implement this as VSP or other operation?
	if (params.hasParam("clustered") && params.getBool("clustered", false) && featureType == FeatureType::POINTS) {
		PointCollection& points = dynamic_cast<PointCollection&>(*features);

		features = clusterPoints(points, params);
	}


	// TODO: startIndex + count
	// TODO: sortBy=attribute  +D or +A
	// push-down?

	// propertyName <- restrict attribute(s)

	// TODO: FILTER (+ FILTER_LANGUAGE)
	// <Filter>
	// 		<Cluster>
	//			<PropertyName>ResolutionHeight</PropertyName><Literal>1234</Literal>
	// 			<PropertyName>ResolutionWidth</PropertyName><Literal>1234</Literal>
	//		</Cluster>
	// </Filter>



	// resolve : ResolveValue = #none  (local+ remote+ all+ none)
	// resolveDepth : UnlimitedInteger = #isInfinite
	// resolveTimeout : TM_Duration = 300s
	//
	// {resolveDepth>0 implies resolve<>#none
	// and resolveTimeout->notEmpty() implies resolve<>#none}

	// resultType =  hits / results

	// outputFormat
	// default is "application/gml+xml; version=3.2"

	//TODO: respect default output format of WFS and support more datatypes
	auto format = params.get("outputformat", "application/json");
	fprintf(stderr, "aaaaaaaaaaa %s %d\n", format.c_str(), format.find(EXPORT_MIME_PREFIX));
	bool exportMode = false;
	if(format.find(EXPORT_MIME_PREFIX) == 0) {
		exportMode = true;
		format = format.substr(strlen(EXPORT_MIME_PREFIX));
	}

	std::string output;
	if (format == "application/json")
		output = features->toGeoJSON(true);
	else if (format == "csv")
		output = features->toCSV();
	else
		throw ArgumentException("WFSService: unknown output format");

	if(exportMode) {
		exportZip(output.c_str(), output.length(), format, *graph->getFullProvenance());
	} else {
		response.sendContentType(format);
		response.finishHeaders();
		response << output;
	}
	// VSPs
	// O
	// A server may implement additional KVP parameters that are not part of this International Standard. These are known as vendor-specific parameters. VSPs allow vendors to specify additional parameters that will enhance the results of requests. A server shall produce valid results even if the VSPs are missing, malformed or if VSPs are supplied that are not known to the server. Unknown VSPs shall be ignored.
	// A server may choose not to advertise some or all of its VSPs. If VSPs are included in the Capabilities XML (see 8.3), the ows:ExtendedCapabilities element (see OGC 06-121r3:2009, 7.4.6) shall be extended accordingly. Additional schema documents may be imported containing the extension(s) of the ows:ExtendedCapabilities element. Any advertised VSP shall include or reference additional metadata describing its meaning (see 8.4). WFS implementers should choose VSP names with care to avoid clashes with WFS parameters defined in this International Standard.
}



std::unique_ptr<PointCollection> WFSService::clusterPoints(const PointCollection &points, const Params &params) const {

	if(params.hasParam("width") || params.hasParam("height")) {
		throw ArgumentException("WFSService: Cluster operation needs width and height specified");
	}

	size_t width, height;

	try {
		width = std::stol(params.get("width"));
		height = std::stol(params.get("height"));
	} catch (const std::invalid_argument &e) {
		throw ArgumentException("WFSService: width and height parameters must be integers");
	}

	if (width <= 0 || height <= 0) {
		throw ArgumentException("WFSService: width or height not valid");
	}

	auto clusteredPoints = make_unique<PointCollection>(points.stref);

	const SpatialReference &sref = points.stref;
	auto x1 = sref.x1;
	auto x2 = sref.x2;
	auto y1 = sref.y1;
	auto y2 = sref.y2;
	auto xres = width;
	auto yres = height;
	pv::CircleClusteringQuadTree clusterer(
			pv::BoundingBox(
					pv::Coordinate((x2 + x1) / (2 * xres),
							(y2 + y1) / (2 * yres)),
					pv::Dimension((x2 - x1) / (2 * xres),
							(y2 - y1) / (2 * yres)), 1), 1);
	for (const Coordinate& pointOld : points.coordinates) {
		clusterer.insert(
				std::make_shared<pv::Circle>(
						pv::Coordinate(pointOld.x / xres,
								pointOld.y / yres), 5, 1));
	}

	// PROPERTYNAME
	// O
	// A list of non-mandatory properties to include in the response.
	// TYPENAMES=(ns1:F1,ns2:F2)(ns1:F1,ns1:F1)&ALIASES=(A,B)(C,D)&FILTER=(<Filter> … for A,B … </Filter>)(<Filter>…for C,D…</Filter>)
	// TYPENAMES=ns1:F1,ns2:F2&ALIASES=A,B&FILTER=<Filter>…for A,B…</Filter>
	// TYPENAMES=ns1:F1,ns1:F1&ALIASES=C,D&FILTER=<Filter>…for C,D…</Filter>

	auto circles = clusterer.getCircles();
	auto &attr_radius = clusteredPoints->feature_attributes.addNumericAttribute("radius", Unit::unknown());
	auto &attr_number = clusteredPoints->feature_attributes.addNumericAttribute("numberOfPoints", Unit::unknown());
	attr_radius.reserve(circles.size());
	attr_number.reserve(circles.size());
	for (auto& circle : circles) {
		size_t idx = clusteredPoints->addSinglePointFeature(Coordinate(circle->getX() * xres,
				circle->getY() * yres));
		attr_radius.set(idx, circle->getRadius());
		attr_number.set(idx, circle->getNumberOfPoints());
	}

	return clusteredPoints;
}

std::pair<FeatureType, Json::Value> WFSService::parseTypeNames(const std::string &typeNames) const {
	// the typeNames parameter specifies the requested layer : typeNames=namespace:featuretype
	// for now the namespace specifies the type of feature (points, lines, polygons) while the featuretype specifies the query

	//split typeNames string

	size_t pos = typeNames.find(":");

	if(pos == std::string::npos)
		throw ArgumentException(concat("WFSService: typeNames delimiter not found", typeNames));

	std::string featureTypeString = typeNames.substr(0, pos);
	std::string queryString = typeNames.substr(pos + 1);

	if(featureTypeString == "")
		throw ArgumentException("WFSService: featureType in typeNames not specified");
	if(queryString == "")
		throw ArgumentException("WFSService: query in typenNames not specified");

	FeatureType featureType = featureTypeConverter.from_string(featureTypeString);

	Json::Reader reader(Json::Features::strictMode());
	Json::Value query;

	if(!reader.parse(queryString, query))
		throw ArgumentException("WFSService: query in typeNames is not valid JSON");

	return std::make_pair(featureType, query);
}
