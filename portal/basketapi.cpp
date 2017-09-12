#include "basketapi.h"

#include "util/curl.h"
#include "util/concat.h"
#include "util/exceptions.h"
#include "util/gfbiodatautil.h"
#include "util/configuration.h"
#include "util/pangaeaapi.h"

#include <cstring>
#include <algorithm>

BasketAPI::Parameter::Parameter(const Json::Value &json) {
	name = json.get("name", "").asString();
	unit = json.get("unitText", "").asString();
	numeric = unit != "";
}

Json::Value BasketAPI::Parameter::toJson() {
	Json::Value json(Json::objectValue);
	json["name"] = name;
	json["unit"] = unit;
	json["numeric"] = numeric;
	return json;
}

BasketAPI::Parameter::Parameter(const std::string &name, const std::string& unit, bool numeric) : name(name), unit(unit), numeric(numeric) {
}


std::vector<BasketAPI::Parameter> BasketAPI::BasketEntry::getParameters(const std::string &dataSetDOI) {
	std::vector<PangaeaAPI::Parameter> pangaeaParameters = PangaeaAPI::getParameters(dataSetDOI);

	std::vector<BasketAPI::Parameter> result;
	for(auto& parameter : pangaeaParameters) {
		result.emplace_back(parameter.name, parameter.unit, parameter.numeric);
	}

	return result;
}


std::unique_ptr<BasketAPI::BasketEntry> BasketAPI::BasketEntry::fromJson(const Json::Value &json, const std::vector<std::string> &availableArchives) {
	std::string metadataLink = json.get("metadatalink", "").asString();

	if(metadataLink.find("doi.pangaea.de/") != std::string::npos) {
		return make_unique<BasketAPI::PangaeaBasketEntry>(json);
	} else {
		return make_unique<BasketAPI::ABCDBasketEntry>(json, availableArchives);
	}
}

BasketAPI::BasketEntry::BasketEntry(const Json::Value &json){
	for(auto& author : json.get("authors", Json::Value(Json::arrayValue))) {
		authors.push_back(author.asString());
	}
	title = json.get("title", "").asString();
	dataCenter = json.get("dataCenter", "").asString();
	dataLink = json.get("dataLink", "").asString();
	metadataLink = json.get("metadatalink", "").asString();

	available = false;
	resultType = ResultType::NONE;
}

Json::Value BasketAPI::BasketEntry::toJson() {
	Json::Value json(Json::objectValue);

	json["authors"] = Json::Value(Json::arrayValue);
	for(auto& author : authors) {
		json["authors"].append(author);
	}

	json["title"] = title;

	json["dataCenter"] = dataCenter;
	json["metadataLink"] = metadataLink;
	json["dataLink"] = dataLink;

	json["available"] = available;

	json["parameters"] = Json::Value(Json::arrayValue);
	for(auto& parameter : parameters) {
		Json::Value jsonParameter = Json::Value(Json::objectValue);
		jsonParameter["name"] = parameter.name;
		jsonParameter["unit"] = parameter.unit;
		jsonParameter["numeric"] = parameter.numeric;
		json["parameters"].append(jsonParameter);
	}

	if(resultType == ResultType::NONE) {
		json["resultType"] = "none";
	} else if(resultType == ResultType::POINTS) {
		json["resultType"] = "points";
	} else if(resultType == ResultType::LINES) {
		json["resultType"] = "lines";
	} else if(resultType == ResultType::POLYGONS) {
		json["resultType"] = "polygons";
	} else if(resultType == ResultType::RASTER) {
		json["resultType"] = "raster";
	}

	return json;
}


BasketAPI::PangaeaBasketEntry::PangaeaBasketEntry(const Json::Value &json) : BasketAPI::BasketEntry(json) {
	doi = metadataLink.substr(metadataLink.find("doi.pangaea.de/") + std::strlen("doi.pangaea.de/"));
	dataLink = json.get("datalink", "").asString();
	format = json.get("format", "").asString();

	isTabSeparated = format.find("text/tab-separated-values") != std::string::npos;

	PangaeaAPI::MetaData metaData = PangaeaAPI::getMetaData(doi);

	// check if parameters LATITUDE and LONGITUDE exist
	bool globalSpatialCoverage = metaData.spatialCoverageType != PangaeaAPI::MetaData::SpatialCoverageType::NONE;
	bool isBox = metaData.spatialCoverageType == PangaeaAPI::MetaData::SpatialCoverageType::BOX;

	bool hasLatitude = false, hasLongitude = false;

	std::string longitudeColumn;
	std::string latitudeColumn;
	for (auto & parameter : metaData.parameters) {
		std::string p = parameter.name;

		if (parameter.isLatitudeColumn()) {
			hasLatitude = true;
			latitudeColumn = p;
		} else if (parameter.isLongitudeColumn()) {
			hasLongitude = true;
			longitudeColumn = p;
		} else {
			parameters.push_back(Parameter(parameter.name, parameter.unit, parameter.numeric));
		}
	}

	isGeoReferenced = globalSpatialCoverage || (hasLatitude && hasLongitude);

	if(isGeoReferenced) {
		if(hasLatitude && hasLongitude) {
			geometrySpecification = GeometrySpecification::XY;
			column_x = longitudeColumn;
			column_y = latitudeColumn;
			resultType = ResultType::POINTS;
		} else {
			geometrySpecification = GeometrySpecification::WKT;

			if(isBox) {
				resultType = ResultType::POLYGONS;
			} else {
				resultType = ResultType::POINTS;
			}

		}
	} else {
		geometrySpecification = GeometrySpecification::NONE;
	}

	available = isTabSeparated && isGeoReferenced;
}


Json::Value BasketAPI::PangaeaBasketEntry::toJson() {
	Json::Value json = BasketEntry::toJson();
	json["type"] = "pangaea";
	json["doi"] = doi;
	json["format"] = format;
	json["isTabSeparated"] = isTabSeparated;
	json["isGeoReferenced"] = isGeoReferenced;

	json["geometrySpecification"] = GeometrySpecificationConverter.to_string(geometrySpecification);
	if(geometrySpecification == GeometrySpecification::XY) {
		json["column_x"] = column_x;
		json["column_y"] = column_y;
	}

	if(resultType == ResultType::NONE) {
		json["resultType"] = "none";
	} else if(resultType == ResultType::POINTS) {
		json["resultType"] = "points";
	} else if(resultType == ResultType::LINES) {
		json["resultType"] = "lines";
	} else if(resultType == ResultType::POLYGONS) {
		json["resultType"] = "polygons";
	} else if(resultType == ResultType::RASTER) {
		json["resultType"] = "raster";
	}

	return json;
}

BasketAPI::ABCDBasketEntry::ABCDBasketEntry(const Json::Value &json, const std::vector<std::string> &availableArchives) : BasketAPI::BasketEntry(json){
	if(json.isMember("parentIdentifier")) {
		// Entry is a unit
		dataLink = json.get("parentIdentifier", "").asString();
		unitId = json.get("dcIdentifier", "").asString();
	} else {
		// Entry is a data set
		dataLink = json.get("datalink", "").asString();
	}

	available = dataLink != "" && std::find(
			availableArchives.begin(),
			availableArchives.end(),
			dataLink)
			!= availableArchives.end();
}

Json::Value BasketAPI::ABCDBasketEntry::toJson() {
	Json::Value json = BasketEntry::toJson();

	json["type"] = "abcd";
	json["unitId"] = unitId;
	json["resultType"] = "points";

	return json;
}

BasketAPI::Basket::Basket(const Json::Value &json, const std::vector<std::string> &availableArchives) {
	query = json.get("queryKeyword", json["queryJSON"][0]["query"]["function_score"]["query"]["filtered"]["query"]["simple_query_string"]["query"]).asString();
	timestamp = json.get("lastModifiedDate", "").asString(); //TODO parse and reformat

	for(auto &basket : json["basketContent"][0]["selected"]) {
		entries.push_back(BasketEntry::fromJson(basket, availableArchives));
	}
}


Json::Value BasketAPI::Basket::toJson() {
	Json::Value json(Json::objectValue);
	json["query"] = query;
	json["timestamp"] = timestamp;
	json["results"] = Json::Value(Json::arrayValue);
	for(auto &entry : entries) {
		json["results"].append(entry->toJson());
	}
	return json;
}



BasketAPI::BasketAPI() {}

BasketAPI::~BasketAPI() {}


std::vector<BasketAPI::Basket> BasketAPI::parseBaskets(Json::Value json) {
	std::vector<std::string> archives = GFBioDataUtil::getAvailableABCDArchives();

	std::vector<Basket> baskets;
	for(auto& portalBasket : json) {
		try {
			Json::Value json(Json::objectValue);

			baskets.emplace_back(portalBasket, archives);
		} catch(const std::exception& ) {
			// ignore malformed baskets
		}
	}

	return std::move(baskets);
}

std::vector<BasketAPI::Basket> BasketAPI::getBaskets(const std::string &userId) {
	// get baskets from portal
	std::stringstream data;
	cURL curl;
	curl.setOpt(CURLOPT_PROXY, Configuration::get("proxy", "").c_str());
	curl.setOpt(CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
	curl.setOpt(CURLOPT_USERPWD, concat(Configuration::get("gfbio.portal.user"), ":", Configuration::get("gfbio.portal.password")).c_str());
	curl.setOpt(CURLOPT_URL, concat(Configuration::get("gfbio.portal.basketwebserviceurl"), "?userId=", userId).c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_WRITEDATA, &data);

	try {
		curl.perform();
	} catch (const cURLException&) {
		throw BasketAPIException("BasketAPI: could not retrieve baskets from portal");
	}

	Json::Reader reader(Json::Features::strictMode());
	Json::Value jsonResponse;
	if (!reader.parse(data.str(), jsonResponse))
		throw BasketAPIException("BasketAPI: could not parse baskets from portal");


	return parseBaskets(jsonResponse);
}



