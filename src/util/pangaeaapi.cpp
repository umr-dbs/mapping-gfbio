#include <algorithm>
#include "pangaeaapi.h"

#include "util/concat.h"
#include "util/stringsplit.h"
#include "util/curl.h"
#include "util/configuration.h"
#include "util/exceptions.h"

PangaeaAPI::Parameter::Parameter(const Json::Value &json, const std::vector<Parameter> &parameters) {
	name = json.get("name", "").asString();
	if(json.isMember("description")) {
		name += " " + json.get("description", "").asString();
	}

	handleNameCollision(parameters);

	unit = json.get("unitText", "").asString();
	numeric = unit != "";
}

void PangaeaAPI::Parameter::handleNameCollision(const std::vector<PangaeaAPI::Parameter> &parameters) {
	std::string originalName = this->name;
	size_t counter = 1;
	bool collision;
	do {
		auto it = find_if(parameters.begin(), parameters.end(),
												 [this](const PangaeaAPI::Parameter & p) -> bool { return p.name ==
																									  this->name;});

		collision = it != parameters.end();
		if(collision) {
			this->name = concat(originalName, counter++);
		}

	} while (collision && counter < 1000);

	if(collision) {
		throw MustNotHappenException("Pangaea Parameter name collision could not be resolved");
	}
}

Json::Value PangaeaAPI::Parameter::toJson() {
	Json::Value json(Json::objectValue);
	json["name"] = name;
	json["unit"] = unit;
	json["numeric"] = numeric;
	return json;
}

std::vector<PangaeaAPI::Parameter> PangaeaAPI::parseParameters(const Json::Value &json) {
	std::vector<PangaeaAPI::Parameter> parameters;
	for(auto &parameter: json.get("variableMeasured", Json::Value(Json::arrayValue))) {
		parameters.emplace_back(parameter, parameters);
	}
	return parameters;
}

Json::Value PangaeaAPI::getMetaDataFromPangaea(const std::string &dataSetDOI) {
	// get parameters from pangae
	std::stringstream data;
	cURL curl;
	curl.setOpt(CURLOPT_PROXY, Configuration::get<std::string>("proxy", "").c_str());
	curl.setOpt(CURLOPT_URL, concat("https://doi.pangaea.de/", dataSetDOI, "?format=metadata_jsonld").c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_WRITEDATA, &data);

	try {
		curl.perform();
	} catch (const cURLException&) {
		throw std::runtime_error(concat("PangaeaAPI: could not retrieve metadata from pangaea doi ", dataSetDOI));
	}

	Json::Reader reader(Json::Features::strictMode());
	Json::Value jsonResponse;
	if (!reader.parse(data.str(), jsonResponse))
		throw std::runtime_error(concat("PangaeaAPI: could not parse metadata from pangaea dataset ", dataSetDOI));

	return jsonResponse;
}

void PangaeaAPI::MetaData::initSpatialCoverage(const Json::Value &json) {
	if(!json.isMember("spatialCoverage")) {
        spatialCoverageType = SpatialCoverageType ::NONE;
    } else {
        Json::Value geo = json["spatialCoverage"]["geo"];

        if (geo.get("@type", "").asString() == "GeoShape") {
            std::vector<std::string> box = split(geo.get("box", "0 0 0 0").asString(), ' ');
            double x1, y1, x2, y2;

            try {
                x1 = std::stod(box[1]);
                y1 = std::stod(box[0]);
                x2 = std::stod(box[3]);
                y2 = std::stod(box[2]);
            } catch (...) {
                x1 = 0.0;
                y1 = 0.0;
                x2 = 0.0;
                y2 = 0.0;
            }

            spatialCoverageType = SpatialCoverageType::BOX;
            spatialCoverageWKT = concat("POLYGON((", x1, " ", y1, ",", x1, " ", y2, ",", x2, " ", y2, ",", x2, " ", y1,
                                        ",", x1, " ", y1, "))");
        } else if (geo.get("@type", "").asString() == "GeoCoordinates") {
            double lon = geo.get("longitude", 0.0).asDouble();
            double lat = geo.get("latitude", 0.0).asDouble();

            spatialCoverageType = SpatialCoverageType::POINT;
            spatialCoverageWKT = concat("POINT(", lon, " ", lat, ")");
        } else {
            spatialCoverageType = SpatialCoverageType::NONE;
        }
    }
}


PangaeaAPI::MetaData::MetaData(const Json::Value &json): parameters(parseParameters(json)) {
	initSpatialCoverage(json);

	license = json.get("license", "").asString();
	url = json.get("url", "").asString();
    metaDataLink = url;

	const Json::Value &creator = json.get("creator", Json::Value(Json::arrayValue));
	if(creator.isArray()) {
		for (auto &author : creator) {
			authors.push_back(author.get("familyName", "").asString() + ", " + author.get("givenName", "").asString());
		}
	} else {
		authors.push_back(creator.get("familyName", "").asString() + ", " + creator.get("givenName", "").asString());
	}

    title = json.get("name", "").asString();

    parseFormat(json);
}

void PangaeaAPI::MetaData::parseFormat(const Json::Value &json) {
    Json::Value distribution = json.get("distribution", Json::Value(Json::arrayValue));

	if(distribution.isArray()) {
		for (auto &aDistribution : distribution) {
			const std::string &fileFormat = aDistribution.get("fileFormat", "").asString();
			if (fileFormat == "text/tab-separated-values") {
				this->format = fileFormat;
				this->dataLink = aDistribution.get("contentUrl", "").asString();
			}
		}

		if(this->format.empty()) {
			const Json::Value &aDistribution = distribution.get(distribution.size() - 1, "unknown");
			this->format = aDistribution.get("fileFormat", "").asString();
			this->dataLink = aDistribution.get("contentUrl", "").asString();
		}
	} else {
		this->format = distribution.get("fileFormat", "").asString();
		this->dataLink = distribution.get("contentUrl", "").asString();
	}
}


PangaeaAPI::MetaData PangaeaAPI::getMetaData(const std::string &dataSetDOI) {
	Json::Value json = PangaeaAPI::getMetaDataFromPangaea(dataSetDOI);

	PangaeaAPI::MetaData metaData(json);

	return metaData;
}

std::string PangaeaAPI::getCitation(const std::string &dataSetDOI) {
	// get parameters from pangaea
	std::stringstream data;
	cURL curl;
	curl.setOpt(CURLOPT_PROXY, Configuration::get<std::string>("proxy", "").c_str());
	curl.setOpt(CURLOPT_URL, concat("https://doi.pangaea.de/", dataSetDOI, "?format=citation_text").c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_WRITEDATA, &data);

	try {
		curl.perform();
	} catch (const cURLException&) {
		throw std::runtime_error("PangaeaAPI: could not retrieve citation from pangaea");
	}

	return data.str();
}


bool PangaeaAPI::Parameter::isLongitudeColumn() const {
	return name == "LONGITUDE" || name.find("Longitude") == 0;
}

bool PangaeaAPI::Parameter::isLatitudeColumn() const {
	return name == "LATITUDE" || name.find("Latitude") == 0;
}