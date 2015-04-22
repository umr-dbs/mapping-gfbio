#ifndef SERVICES_WFS_REQUEST_H_
#define SERVICES_WFS_REQUEST_H_

#include <string>
#include <cmath>
#include <map>
#include <sstream>
#include <json/json.h>

#include "datatypes/raster.h"
#include "operators/operator.h"
#include "datatypes/multipointcollection.h"
#include "pointvisualization/CircleClusteringQuadTree.h"

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

#endif /* SERVICES_WFS_REQUEST_H_ */
