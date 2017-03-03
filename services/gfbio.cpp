
#include "services/httpservice.h"
#include "userdb/userdb.h"
#include "util/configuration.h"
#include "util/concat.h"
#include "util/exceptions.h"
#include "util/curl.h"
#include "util/gfbiodatautil.h"

#include <cstring>
#include <sstream>
#include <json/json.h>
#include <algorithm>
#include <pqxx/pqxx>

#include <fstream>

/*
 * This class provides methods for GFBio users
 *
 * Operations:
 * - request = login: login using gfbio portal token
 *   - parameters:
 *     - token
 * - request = baskets: get baskets from portal
 * - request = abcd: get list of available abcd archives
 */
class GFBioService : public HTTPService {
public:
	using HTTPService::HTTPService;
	virtual ~GFBioService() = default;

	struct GFBioServiceException
		: public std::runtime_error { using std::runtime_error::runtime_error; };

private:

	class PangaeaParameter {
		public:
		PangaeaParameter(const std::string &identifier,
				const std::string &fullName, const std::string &shortName,
				const std::string &unit) :
				identifier(identifier), fullName(fullName), shortName(
						shortName), unit(unit) {

		}
			std::string identifier;
			std::string fullName;
			std::string shortName;
			std::string unit;
	};

	PangaeaParameter getPangaeaParameterByFullName(pqxx::connection &connection, const std::string &fullName);

	size_t authenticateWithPortal(const std::string &token);
	Json::Value getUserDetailsFromPortal(const size_t userId);

	Json::Value getGFBioDataCentersJSON();

	static constexpr const char* EXTERNAL_ID_PREFIX = "GFBIO:";

	virtual void run();
};

REGISTER_HTTP_SERVICE(GFBioService, "gfbio");


/**
 * read the GFBio data centers file and return as Json object
 * TODO: manage data centers in a database and map them to a c++ class
 * @return a json object containing the available data centers
 */
Json::Value GFBioService::getGFBioDataCentersJSON() {
	auto path = Configuration::get("gfbio.abcd.datapath");

	std::ifstream file(path + "gfbio_datacenters.json");
	if (!file.is_open()) {
		throw GFBioServiceException("gfbio_datacenters.json missing");
	}

	Json::Reader reader(Json::Features::strictMode());
	Json::Value root;
	if (!reader.parse(file, root)) {
		throw GFBioServiceException("gfbio_datacenters.json invalid");
	}

	return root;
}

/**
 * authenticate user token with portal
 * @return portaluserId of the user
 */
size_t GFBioService::authenticateWithPortal(const std::string &token) {
	std::stringstream data;
	cURL curl;
	curl.setOpt(CURLOPT_PROXY, Configuration::get("proxy", "").c_str());
	curl.setOpt(CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
	curl.setOpt(CURLOPT_USERPWD, concat(Configuration::get("gfbio.portal.user"), ":", Configuration::get("gfbio.portal.password")).c_str());
	curl.setOpt(CURLOPT_URL, (Configuration::get("gfbio.portal.authenticateurl") + "/token/" + token).c_str());

	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_POST, 1);

	curl.setOpt(CURLOPT_POSTFIELDS, ("token=" + token).c_str());
	curl.setOpt(CURLOPT_WRITEDATA, &data);


	try {
		curl.perform();
	} catch (const cURLException& e) {
		throw GFBioService::GFBioServiceException("GFBioService: Portal unavailable");
	}

	Json::Reader reader(Json::Features::strictMode());
	Json::Value response;
	if (!reader.parse(data.str(), response))
		throw GFBioService::GFBioServiceException("GFBioService: Portal response invalid (malformed JSON)");


	 // return 0 : success, 1 : token expired,
	 // 2 : no record found, 3 non-admin user,
	 // 4 : unknown error;
	if(response.size() == 1 && response[0].get("success", 4).asInt() == 0) {
		if(!response[0].isMember("userid"))
			throw GFBioService::GFBioServiceException("GFBioService: Portal response invalid (missing userId)");

		return response[0]["userid"].asInt();
	} else
		throw GFBioService::GFBioServiceException("GFBioService: wrong portal credentials");
}

/**
 * get user details from portal for given userId
 * @return the first element from the portal's JSON response array
 */
Json::Value GFBioService::getUserDetailsFromPortal(const size_t userId) {
	std::stringstream data;
	cURL curl;
	curl.setOpt(CURLOPT_PROXY, Configuration::get("proxy", "").c_str());
	curl.setOpt(CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
	curl.setOpt(CURLOPT_USERPWD, concat(Configuration::get("gfbio.portal.user"), ":", Configuration::get("gfbio.portal.password")).c_str());
	curl.setOpt(CURLOPT_URL, (concat(Configuration::get("gfbio.portal.userdetailswebserviceurl"), "?userId=", userId)).c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_WRITEDATA, &data);

	try {
		curl.perform();
	} catch (const cURLException& e) {
		throw GFBioService::GFBioServiceException("GFBioService: Portal unavailable");
	}

	Json::Reader reader(Json::Features::strictMode());
	Json::Value response;
	if (!reader.parse(data.str(), response) || response.size() < 1 || !response[0].isMember("emailAddress"))
		throw GFBioService::GFBioServiceException("GFBioService: Portal response invalid (malformed JSON)");

	return response[0];
}

/**
 * get pangaea parameter by full name from postgres table pangaea.parameters
 * throws Exception if not parameter is found
 */
GFBioService::PangaeaParameter GFBioService::getPangaeaParameterByFullName(pqxx::connection &connection, const std::string &fullName) {
	connection.prepare("pangaea_parameter", "SELECT identifier, short_name, coalesce(unit, '') as unit FROM pangaea.parameters WHERE full_name = $1");
	pqxx::work work(connection);

	pqxx::result result = work.prepared("pangaea_parameter")(fullName).exec();

	if (result.size() < 1)
		throw GFBioServiceException("GFBioService: pangaea parameter not found:" + fullName);

	auto row = result[0];
	return PangaeaParameter(row[0].as<std::string>(), fullName, row[1].as<std::string>(), row[2].as<std::string>());
}

void GFBioService::run() {
	try {
		std::string request = params.get("request");

		if(request == "login"){
			// login to the vat system using GFBio portal token
			std::string token = params.get("token");

			size_t gfbioId = authenticateWithPortal(token);
			std::string externalId = EXTERNAL_ID_PREFIX + std::to_string(gfbioId);

			std::shared_ptr<UserDB::Session> session;
			try {
				// create session for user if he already exists
				session = UserDB::createSessionForExternalUser(externalId, 8 * 3600);
			} catch (const UserDB::authentication_error& e) {
				// user does not exist locally => create
				Json::Value userDetails = getUserDetailsFromPortal(gfbioId);
				try {
					auto user = UserDB::createExternalUser(userDetails.get("emailAddress", "").asString(),
											   userDetails.get("firstName", "").asString() + " " + userDetails.get("lastName", "").asString(),
											   userDetails.get("emailAddress", "").asString(), externalId);

					try {
						auto gfbioGroup = UserDB::loadGroup("gfbio");
						user->joinGroup(*gfbioGroup);
					} catch (const UserDB::database_error&) {
						auto gfbioGroup = UserDB::createGroup("gfbio");
						user->joinGroup(*gfbioGroup);
					}

					session = UserDB::createSessionForExternalUser(externalId, 8 * 3600);
				} catch (const std::exception&) {
					throw GFBioService::GFBioServiceException("GFBioService: Could not create new user from GFBio portal.");
				}
			}

			response.sendSuccessJSON("session", session->getSessiontoken());
			return;
		}

		// helper methods for gbif source
		if(request == "searchSpecies") {
			std::string term = params.get("term");
			if(term.size() < 3) {
				response.sendFailureJSON("Term has to be >= 3 characters");
				return;
			}

			pqxx::connection connection (Configuration::get("operators.gfbiosource.dbcredentials"));

			connection.prepare("searchSpecies", "SELECT DISTINCT name FROM gbif.gbif_taxon_to_name WHERE lower(name) like lower($1) ORDER BY name ASC");
			pqxx::work work(connection);
			pqxx::result result = work.prepared("searchSpecies")(term + "%").exec();

			Json::Value json(Json::objectValue);
			Json::Value names(Json::arrayValue);
			for(size_t i = 0; i < result.size(); ++i) {
				auto row = result[i];
				names.append(row[0].as<std::string>());
			}

			json["speciesNames"] = names;
			response.sendSuccessJSON(json);
			return;
		}

		if(request == "queryDataSources") {
			std::string scientificName = params.get("term");
			if(scientificName.size() < 3) {
				response.sendFailureJSON("Term has to be >= 3 characters");
				return;
			}

			Json::Value json(Json::objectValue);
			Json::Value sources(Json::arrayValue);

			Json::Value gbif(Json::objectValue);
			gbif["name"] = "GBIF";
			gbif["count"] = (Json::Int) GFBioDataUtil::countGBIFResults(scientificName);

			sources.append(gbif);

			Json::Value iucn(Json::objectValue);
			iucn["name"] = "IUCN";
			iucn["count"] = (Json::Int) GFBioDataUtil::countIUCNResults(scientificName);

			sources.append(iucn);

			json["dataSources"] = sources;

			response.sendSuccessJSON(json);
			return;
		}

		if(request == "abcd") {
			Json::Value dataCenters = getGFBioDataCentersJSON();

			response.sendSuccessJSON(dataCenters);
			return;
		}


		//protected methods
		auto session = UserDB::loadSession(params.get("sessiontoken"));

		if (request == "baskets") {
			// connection for resolving pangaea parameters
			pqxx::connection connection (Configuration::get("operators.gfbiosource.dbcredentials"));

			std::string gfbioId = session->getUser().getExternalid();
			if(gfbioId.find(EXTERNAL_ID_PREFIX) != 0)
				throw GFBioServiceException("GFBioService: This service is only available for GFBio user.");

			gfbioId = gfbioId.substr(strlen(EXTERNAL_ID_PREFIX));

			//TODO: cache baskets locally
			//TODO: implement pagination

			// get baskets from portal
			std::stringstream data;
			cURL curl;
			curl.setOpt(CURLOPT_PROXY, Configuration::get("proxy", "").c_str());
			curl.setOpt(CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
			curl.setOpt(CURLOPT_USERPWD, concat(Configuration::get("gfbio.portal.user"), ":", Configuration::get("gfbio.portal.password")).c_str());
			curl.setOpt(CURLOPT_URL, concat(Configuration::get("gfbio.portal.basketwebserviceurl"), "?userId=", gfbioId).c_str());
			curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
			curl.setOpt(CURLOPT_WRITEDATA, &data);

			try {
				curl.perform();
			} catch (const cURLException&) {
				throw GFBioServiceException("GFBioService: could not retrieve baskets from portal");
			}

			Json::Reader reader(Json::Features::strictMode());
			Json::Value jsonResponse;
			if (!reader.parse(data.str(), jsonResponse))
				throw GFBioServiceException("GFBioService: could not parse baskets from portal");


			// get available abcd archives
			Json::Value dataCenters = getGFBioDataCentersJSON();
			std::vector<std::string> availableArchives;
			for(Json::Value &dataCenter : dataCenters["archives"]) {
				availableArchives.push_back(dataCenter.get("file", "").asString());
			}

			// parse relevant info, build mapping response
			Json::Value jsonBaskets(Json::arrayValue);
			for(auto portalBasket : jsonResponse) {
				try {
					Json::Value basket(Json::objectValue);
					basket["query"] = portalBasket["queryJSON"][0]["query"]["function_score"]["query"]["filtered"]["query"]["simple_query_string"]["query"].asString();
					basket["timestamp"] = portalBasket["lastModifiedDate"].asString(); //TODO parse and reformat

					basket["results"] = Json::Value(Json::arrayValue);
					for(auto result : portalBasket["basketContent"][0]["selected"]) {
						Json::Value entry(Json::objectValue);
						entry["title"] = result["title"];
						entry["authors"] = result["authors"];
						entry["dataCenter"] = result["dataCenter"];
						std::string metadataLink = result["metadatalink"].asString();
						entry["metadataLink"] = metadataLink;


						// better way to determine dataCenter that's equally robust?
						if(metadataLink.find("doi.pangaea.de/") != std::string::npos) {
							// entry is from pangaea
							entry["type"] = "pangaea";
							entry["doi"] = metadataLink.substr(metadataLink.find("doi.pangaea.de/") + strlen("doi.pangaea.de/"));
							entry["dataLink"] = result["datalink"];
							entry["format"] = result["format"];

							bool isTabSeparated = entry["format"].asString().find("text/tab-separated-values") != std::string::npos;


							// check if parameters LATITUDE and LONGITUDE exist
							bool isGeoReferenced = false;
							if (isTabSeparated && result.isMember("parameter")) {
								bool hasLatitude = false, hasLongitude = false;

								auto parameters = result["parameter"];
								for (Json::ValueIterator parameter = parameters.begin(); parameter != parameters.end(); ++parameter) {
									std::string p = (*parameter).asString();

									if (p == "LATITUDE" || p.find("Latitude") != std::string::npos)
										hasLatitude = true;
									else if (p == "LONGITUDE" || p.find("Longitude") != std::string::npos)
										hasLongitude = true;
								}

								isGeoReferenced = hasLatitude && hasLongitude;
							}

							// TODO: handle geocodes

							//resolve parameters
							auto parameters = result["parameter"];
							Json::Value entryParameters(Json::arrayValue);
							for (Json::ValueIterator parameter = parameters.begin(); parameter != parameters.end(); ++parameter) {
								Json::Value jsonParameter(Json::objectValue);

								std::string parameterString = (*parameter).asString();
								try {
									PangaeaParameter pangaeaParameter = getPangaeaParameterByFullName(connection, parameterString);

									jsonParameter["name"] = pangaeaParameter.fullName;
									jsonParameter["unit"] = pangaeaParameter.unit;
									jsonParameter["numeric"] = pangaeaParameter.unit != "";
									entryParameters.append(jsonParameter);
								} catch(const GFBioServiceException& e) {

									// TODO handle geocodes
									if (parameterString == "Latitude of event" || parameterString == "Longitude of event") {
										jsonParameter["name"] = parameterString;
										jsonParameter["unit"] = "";
										jsonParameter["numeric"] = true;
										entryParameters.append(jsonParameter);
									}

									fprintf(stderr, e.what());
								}


							}
							entry["parameters"] = entryParameters;

							entry["isTabSeparated"] = isTabSeparated;
							entry["isGeoreferenced"] = isGeoReferenced;
							entry["available"] = isTabSeparated && isGeoReferenced;

						} else {
							entry["type"] = "abcd";
							entry["dataLink"] = result["parentIdentifier"];
							entry["unitId"] = result["dcIdentifier"];

							entry["available"] = std::find(
									availableArchives.begin(),
									availableArchives.end(),
									entry["dataLink"].asString())
									!= availableArchives.end();
						}

						basket["results"].append(entry);
					}
					jsonBaskets.append(basket);
				} catch(const std::exception& ) {
					// ignore malformed baskets
				}
			}

			Json::Value json(Json::objectValue);
			json["baskets"] = jsonBaskets;
			response.sendSuccessJSON(json);

			return;
		}
		response.sendFailureJSON("GFBioService: Invalid request");
	}
	catch (const std::exception &e) {
		response.sendFailureJSON(e.what());
	}
}
