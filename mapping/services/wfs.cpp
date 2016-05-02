
// TODO: this needs to be ported to the new architecture.

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

enum WFSServiceType {
	GetCapabilities, GetFeature
};

enum FeatureType {
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
		WFSService() = default;
		virtual ~WFSService() = default;

		std::string getResponse(const Params &params);

		virtual void run(const Params& params, HTTPResponseStream& result, std::ostream &error) {
			result.sendContentType("application/json");
			result.finishHeaders();
			result << getResponse(params);
		}

	private:
		std::string getCapabilities();
		std::string getFeature(const Params &params);

		// TODO: implement
		std::string describeFeatureType();
		std::string getPropertyValue();
		std::string listStoredQueries();
		std::string describeStoredQueries();

		// helper functions
		std::pair<FeatureType, Json::Value> parseTypeNames(const std::string &typeNames) const;
		SpatialReference parseBBOX(SpatialReference &sref, const std::string bbox_str) const;
		TemporalReference time_from_params(const Params &params) const;
		bool to_bool(std::string str) const;
		epsg_t epsg_from_param(const std::string &crs, epsg_t def = EPSG_WEBMERCATOR) const;
		epsg_t epsg_from_param(const Params &params, const std::string &key, epsg_t def = EPSG_WEBMERCATOR) const;
		std::unique_ptr<PointCollection> clusterPoints(const PointCollection &points, const Params &params) const;

		const std::map<std::string, WFSServiceType> stringToRequest {
			{"GetCapabilities", WFSServiceType::GetCapabilities},
			{"GetFeature", WFSServiceType::GetFeature}
		};


};
REGISTER_HTTP_SERVICE(WFSService, "WFS");


std::string WFSService::getResponse(const Params &parameters) {
	if (parameters.count("service") <= 0 && parameters.get("service") != "WFS") {
		return "wrong service"; // TODO: Error message
	}

	if (parameters.count("version") <= 0 && parameters.get("version") != "2.0.0") {
		return "wrong version"; // TODO: Error message
	}

	switch (stringToRequest.at(parameters.get("request"))) {
	case WFSServiceType::GetCapabilities:
		return getCapabilities();

		break;
	case WFSServiceType::GetFeature:
		return getFeature(parameters);

		break;
	default:

		return "wrong request"; // TODO exception
		break;
	}
}

std::string WFSService::getCapabilities() {
	return "";
}

std::string WFSService::getFeature(const Params& parameters) {
	if(parameters.count("typenames") == 0)
		throw ArgumentException("WFSService: typeNames parameter not specified");

	std::pair<FeatureType, Json::Value> typeNames = parseTypeNames(parameters.get("typenames"));
	FeatureType featureType = typeNames.first;
	Json::Value query = typeNames.second;

	TemporalReference tref = time_from_params(parameters);

	// srsName=CRS
	// TODO: this parameter is optional in WFS, but we use it here to create the Spatial Reference. Is there another way to get it from the querygraph?
	if(parameters.count("srsname") == 0)
		throw new ArgumentException("WFSService: Parameter srsname is missing");
	epsg_t queryEpsg = this->epsg_from_param(parameters, "srsname");


	SpatialReference sref(queryEpsg);
	if(parameters.count("bbox") > 0 ) {
		parseBBOX(sref, parameters.at("bbox"));
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
	if (parameters.count("clustered") > 0 && this->to_bool(parameters.get("clustered")) && featureType == FeatureType::POINTS) {
		PointCollection& points = dynamic_cast<PointCollection&>(*features);

		features = clusterPoints(points, parameters);
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
	return features->toGeoJSON(true);

	// VSPs
	// O
	// A server may implement additional KVP parameters that are not part of this International Standard. These are known as vendor-specific parameters. VSPs allow vendors to specify additional parameters that will enhance the results of requests. A server shall produce valid results even if the VSPs are missing, malformed or if VSPs are supplied that are not known to the server. Unknown VSPs shall be ignored.
	// A server may choose not to advertise some or all of its VSPs. If VSPs are included in the Capabilities XML (see 8.3), the ows:ExtendedCapabilities element (see OGC 06-121r3:2009, 7.4.6) shall be extended accordingly. Additional schema documents may be imported containing the extension(s) of the ows:ExtendedCapabilities element. Any advertised VSP shall include or reference additional metadata describing its meaning (see 8.4). WFS implementers should choose VSP names with care to avoid clashes with WFS parameters defined in this International Standard.
}

std::unique_ptr<PointCollection> WFSService::clusterPoints(const PointCollection &points, const Params &params) const {

	if(params.count("width") == 0 || params.count("height") == 0) {
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
		throw ArgumentException("WFSService: output_width or output_height not valid");
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

TemporalReference WFSService::time_from_params(const Params &parameters) const {
	TemporalReference tref(TIMETYPE_UNIX);
	if(parameters.count("time") > 0){
		// from: http://www.ogcnetwork.net/node/178
		// "Constrain the results to return values within these _slash-separated_
		// timestamps. Specified using this format: 2006-10-23T04:05:06 -0500/2006-10-25T04:05:06 -0500
		// Either the start value or the end value can be omitted to indicate no restriction on time in that direction."
		const std::string &timeString = parameters.at("time");
		auto timeParser = TimeParser::create(TimeParser::Format::ISO);
		size_t sep = timeString.find("/");

		if(sep == std::string::npos){
			tref.t1 = timeParser->parse(timeString);
		} else if (sep == 0){
			tref.t2 = timeParser->parse(timeString.substr(1));
		}
		else {
			tref.t1 = timeParser->parse(timeString.substr(0, sep));
			if(sep < timeString.length() - 1)
				tref.t2 = timeParser->parse(timeString.substr(sep + 1));
		}

		tref.validate();
	}
	return tref;
}

SpatialReference WFSService::parseBBOX(SpatialReference &sref, const std::string bbox_str) const {
	//"The format of the BBOX parameter is bbox=a1,b1,a2,b2``where ``a1, b1, a2, and b2 represent the coordinate values."

	//extract coordinate
	std::vector<std::string> coordinates;
	coordinates.reserve(4);
	size_t start = 0;
	size_t stop;
	do {
		stop = bbox_str.find(",", start);
		coordinates.push_back(bbox_str.substr(start, stop - start));
		start = stop + 1;
	} while(stop != std::string::npos);

	if(coordinates.size() != 4)
		throw ArgumentException("WFSService: BBox has to consist of 4 coordinate");

	//parse coordinates
	double a1, b1, a2, b2;
	try {
		a1 = std::stod(coordinates[0]);
		b1 = std::stod(coordinates[1]);
		a2 = std::stod(coordinates[2]);
		b2 = std::stod(coordinates[3]);
	} catch(const std::invalid_argument &e) {
		throw ArgumentException("WFSService: Could not parse bbox coordinates");
	}

	//TODO: "The order of coordinates passed to the BBOX parameter depends on the coordinate system used."
	sref.x1 = a1;
	sref.y1 = b1;
	sref.x2 = a2;
	sref.y2 = b2;

	return sref;
}

bool WFSService::to_bool(std::string str) const {
	std::transform(str.begin(), str.end(), str.begin(), ::tolower);
	std::istringstream is(str);
	bool b;
	is >> std::boolalpha >> b;
	return b;
}

epsg_t WFSService::epsg_from_param(const std::string &crs, epsg_t def) const {
	if (crs == "")
		return def;
	if (crs.compare(0, 5, "EPSG:") == 0)
		return (epsg_t) std::stoi(crs.substr(5, std::string::npos));
	throw ArgumentException("Unknown CRS specified");
}

epsg_t WFSService::epsg_from_param(const Params &params, const std::string &key, epsg_t def) const {
	if (params.count(key) < 1)
		return def;
	return epsg_from_param(params.at(key), def);
}
