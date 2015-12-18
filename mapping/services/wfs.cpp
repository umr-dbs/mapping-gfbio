
// TODO: this needs to be ported to the new architecture.

#include "services/ogcservice.h"
#include "datatypes/pointcollection.h"
#include "operators/operator.h"
#include "pointvisualization/CircleClusteringQuadTree.h"
#include "util/timeparser.h"

#include <string>
#include <cmath>
#include <map>
#include <sstream>
#include <json/json.h>

enum WFSRequestType {
	GetCapabilities, GetFeature
};

class WFSRequest {
public:
	WFSRequest(std::map<std::string, std::string> parameters);
	virtual ~WFSRequest();

	auto getResponse() -> std::string;
private:
	auto getCapabilities() -> std::string;
	auto getFeature() -> std::string;

	// TODO: implement
	auto describeFeatureType() const -> std::string;
	auto getPropertyValue() const -> std::string;
	auto listStoredQueries() const -> std::string;
	auto describeStoredQueries() const -> std::string;

	std::map<std::string, std::string> parameters;

	// helper functions
	auto parseBBOX(double *bbox, const std::string bbox_str, epsg_t epsg = EPSG_WEBMERCATOR, bool allow_infinite = false) const -> void;
	auto to_bool(std::string str) const -> bool;
	auto epsg_from_param(const std::string &crs, epsg_t def = EPSG_WEBMERCATOR) const -> epsg_t;
	auto epsg_from_param(const std::map<std::string, std::string> &params, const std::string &key, epsg_t def = EPSG_WEBMERCATOR) const -> epsg_t;

	// TODO: extract commons to utils class

	const std::map<std::string, WFSRequestType> stringToRequest {
		{"GetCapabilities", WFSRequestType::GetCapabilities},
		{"GetFeature", WFSRequestType::GetFeature}
	};
};


class WFSService : public OGCService {
	public:
		WFSService() = default;
		virtual ~WFSService() = default;
		virtual void run(const Params& params, HTTPResponseStream& result, std::ostream &error) {
		  result.sendContentType("application/json");
		  result.finishHeaders();
		  result << WFSRequest(params).getResponse();
		}
};
REGISTER_HTTP_SERVICE(WFSService, "WFS");


WFSRequest::WFSRequest(std::map<std::string, std::string> parameters) :
		parameters(parameters) {
}

WFSRequest::~WFSRequest() {
}

auto WFSRequest::getResponse() -> std::string {
	if (parameters.count("service") <= 0 && parameters["service"] != "WFS") {
		return "wrong service"; // TODO: Error message
	}

	if (parameters.count("version") <= 0 && parameters["version"] != "2.0.0") {
		return "wrong version"; // TODO: Error message
	}

	switch (stringToRequest.at(parameters["request"])) {
	case WFSRequestType::GetCapabilities:
		return getCapabilities();

		break;
	case WFSRequestType::GetFeature:
		return getFeature();

		break;
	default:

		return "wrong request"; // TODO exception
		break;
	}
}

auto WFSRequest::getCapabilities() -> std::string {
	return "";
}

auto WFSRequest::getFeature() -> std::string {
	// read featureId
	Json::Reader jsonReader{Json::Features::strictMode()};
	Json::Value featureId;
	if (!jsonReader.parse(parameters["featureid"], featureId)) {
		return "unable to parse json of featureId";
	}

	int output_width = featureId["width"].asInt();
	int output_height = featureId["height"].asInt();
	if (output_width <= 0 || output_height <= 0) {
		return "output_width or output_height not valid";
	}

	TemporalReference tref(TIMETYPE_UNIX);
	if(parameters.count("time") > 0){
		// from: http://www.ogcnetwork.net/node/178
		// "Constrain the results to return values within these _slash-separated_
		// timestamps. Specified using this format: 2006-10-23T04:05:06 -0500/2006-10-25T04:05:06 -0500
		// Either the start value or the end value can be omitted to indicate no restriction on time in that direction."
		std::string &timeString = parameters["time"];
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

	// srsName=CRS
	epsg_t queryEpsg = this->epsg_from_param(parameters, "srsname");

	double bbox[4];
	this->parseBBOX(bbox, parameters.at("bbox"), queryEpsg, true);

	auto graph = GenericOperator::fromJSON(featureId["query"]);

	// TODO: typeNames
	// namespace + points or polygons

	QueryProfiler profiler;
	bool flipx, flipy;

	QueryRectangle rect(
		SpatialReference(queryEpsg, bbox[0], bbox[1], bbox[2], bbox[3], flipx, flipy),
		tref,
		QueryResolution::none()
	);
	auto points = graph->getCachedPointCollection(rect, profiler);

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
	if (this->to_bool(parameters["clustered"]) == true) {
		auto clusteredPoints = make_unique<PointCollection>(points->stref);

		auto x1 = bbox[0];
		auto x2 = bbox[2];
		auto y1 = bbox[1];
		auto y2 = bbox[3];
		auto xres = output_width;
		auto yres = output_height;
		pv::CircleClusteringQuadTree clusterer(
				pv::BoundingBox(
						pv::Coordinate((x2 + x1) / (2 * xres),
								(y2 + y1) / (2 * yres)),
						pv::Dimension((x2 - x1) / (2 * xres),
								(y2 - y1) / (2 * yres)), 1), 1);
		for (Coordinate& pointOld : points->coordinates) {
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

		points.swap(clusteredPoints);
	}

	// resolve : ResolveValue = #none  (local+ remote+ all+ none)
	// resolveDepth : UnlimitedInteger = #isInfinite
	// resolveTimeout : TM_Duration = 300s
	//
	// {resolveDepth>0 implies resolve<>#none
	// and resolveTimeout->notEmpty() implies resolve<>#none}

	// resultType =  hits / results

	// outputFormat
	// default is "application/gml+xml; version=3.2"
	return points->toGeoJSON(true);

	// VSPs
	// O
	// A server may implement additional KVP parameters that are not part of this International Standard. These are known as vendor-specific parameters. VSPs allow vendors to specify additional parameters that will enhance the results of requests. A server shall produce valid results even if the VSPs are missing, malformed or if VSPs are supplied that are not known to the server. Unknown VSPs shall be ignored.
	// A server may choose not to advertise some or all of its VSPs. If VSPs are included in the Capabilities XML (see 8.3), the ows:ExtendedCapabilities element (see OGC 06-121r3:2009, 7.4.6) shall be extended accordingly. Additional schema documents may be imported containing the extension(s) of the ows:ExtendedCapabilities element. Any advertised VSP shall include or reference additional metadata describing its meaning (see 8.4). WFS implementers should choose VSP names with care to avoid clashes with WFS parameters defined in this International Standard.
}

auto WFSRequest::parseBBOX(double *bbox, const std::string bbox_str, epsg_t epsg, bool allow_infinite) const -> void {
	// &BBOX=0,0,10018754.171394622,10018754.171394622
	for (int i = 0; i < 4; i++)
		bbox[i] = NAN;

	// Figure out if we know the extent of the CRS
	// WebMercator, http://www.easywms.com/easywms/?q=en/node/3592
	//                               minx          miny         maxx         maxy
	double extent_webmercator[4] { -20037508.34, -20037508.34, 20037508.34,
			20037508.34 };
	double extent_latlon[4] { -180, -90, 180, 90 };
	double extent_msg[4] { -5568748.276, -5568748.276, 5568748.276, 5568748.276 };

	double *extent = nullptr;
	if (epsg == EPSG_WEBMERCATOR)
		extent = extent_webmercator;
	else if (epsg == EPSG_LATLON)
		extent = extent_latlon;
	else if (epsg == EPSG_GEOSMSG)
		extent = extent_msg;

	std::string delimiters = " ,";
	size_t current, next = -1;
	int element = 0;
	do {
		current = next + 1;
		next = bbox_str.find_first_of(delimiters, current);
		std::string stringValue = bbox_str.substr(current, next - current);
		double value = 0;

		if (stringValue == "Infinity") {
			if (!allow_infinite)
				throw ArgumentException("cannot process BBOX with Infinity");
			if (!extent)
				throw ArgumentException(
						"cannot process BBOX with Infinity and unknown CRS");
			value = std::max(extent[element], extent[(element + 2) % 4]);
		} else if (stringValue == "-Infinity") {
			if (!allow_infinite)
				throw ArgumentException("cannot process BBOX with Infinity");
			if (!extent)
				throw ArgumentException(
						"cannot process BBOX with Infinity and unknown CRS");
			value = std::min(extent[element], extent[(element + 2) % 4]);
		} else {
			value = std::stod(stringValue);
			if (!std::isfinite(value))
				throw ArgumentException(
						"BBOX contains entry that is not a finite number");
		}

		bbox[element++] = value;
	} while (element < 4 && next != std::string::npos);

	if (element != 4)
		throw ArgumentException("Could not parse BBOX parameter");

	/*
	 * OpenLayers insists on sending latitude in x and longitude in y.
	 * The MAPPING code (including gdal's projection classes) don't agree: east/west should be in x.
	 * The simple solution is to swap the x and y coordinates.
	 */
	if (epsg == EPSG_LATLON) {
		std::swap(bbox[0], bbox[1]);
		std::swap(bbox[2], bbox[3]);
	}

	// If no extent is known, just trust the client.
	if (extent) {
		double bbox_normalized[4];
		for (int i = 0; i < 4; i += 2) {
			bbox_normalized[i] = (bbox[i] - extent[0])
					/ (extent[2] - extent[0]);
			bbox_normalized[i + 1] = (bbox[i + 1] - extent[1])
					/ (extent[3] - extent[1]);
		}

		// Koordinaten können leicht ausserhalb liegen, z.B.
		// 20037508.342789, 20037508.342789
		for (int i = 0; i < 4; i++) {
			if (bbox_normalized[i] < 0.0 && bbox_normalized[i] > -0.001)
				bbox_normalized[i] = 0.0;
			else if (bbox_normalized[i] > 1.0 && bbox_normalized[i] < 1.001)
				bbox_normalized[i] = 1.0;
		}

		for (int i = 0; i < 4; i++) {
			if (bbox_normalized[i] < 0.0 || bbox_normalized[i] > 1.0)
				throw ArgumentException("BBOX exceeds extent");
		}
	}

	//bbox_normalized[1] = 1.0 - bbox_normalized[1];
	//bbox_normalized[3] = 1.0 - bbox_normalized[3];
}

auto WFSRequest::to_bool(std::string str) const -> bool {
	std::transform(str.begin(), str.end(), str.begin(), ::tolower);
	std::istringstream is(str);
	bool b;
	is >> std::boolalpha >> b;
	return b;
}

auto WFSRequest::epsg_from_param(const std::string &crs, epsg_t def) const -> epsg_t {
	if (crs == "")
		return def;
	if (crs.compare(0, 5, "EPSG:") == 0)
		return (epsg_t) std::stoi(crs.substr(5, std::string::npos));
	throw ArgumentException("Unknown CRS specified");
}

auto WFSRequest::epsg_from_param(const std::map<std::string, std::string> &params, const std::string &key, epsg_t def) const -> epsg_t {
	if (params.count(key) < 1)
		return def;
	return epsg_from_param(params.at(key), def);
}
