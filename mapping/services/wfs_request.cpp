/*
 * WFSRequest.cpp
 *
 *  Created on: 13.03.2015
 *      Author: beilschmidt
 */

#include "wfs_request.h"

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

	time_t timestamp = 42; // TODO: default value
	if (featureId["timestamp"].isInt()) {
		timestamp = static_cast<long>(featureId["timestamp"].asInt64());
	}

	// srsName=CRS
	epsg_t queryEpsg = this->epsg_from_param(parameters, "srsname");

	double bbox[4];
	this->parseBBOX(bbox, parameters.at("bbox"), queryEpsg, true);

	auto graph = GenericOperator::fromJSON(featureId["query"]);

	// TODO: typeNames
	// namespace + points or polygons

	QueryProfiler profiler;
	auto points = graph->getCachedMultiPointCollection(
			QueryRectangle(timestamp, bbox[0], bbox[1], bbox[2], bbox[3],
					output_width, output_height, queryEpsg), profiler);

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
		auto clusteredPoints = std::make_unique<MultiPointCollection>(points->stref);

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
		clusteredPoints->local_md_value.addVector("radius", circles.size());
		clusteredPoints->local_md_value.addVector("numberOfPoints",
				circles.size());
		for (auto& circle : circles) {
			size_t idx = clusteredPoints->addFeature(Coordinate(circle->getX() * xres,
					circle->getY() * yres));
			clusteredPoints->local_md_value.set(idx, "radius",
					circle->getRadius());
			clusteredPoints->local_md_value.set(idx, "numberOfPoints",
					circle->getNumberOfPoints());
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
