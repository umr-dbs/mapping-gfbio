
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
	size_t authenticateWithPortal(const std::string &token);
	Json::Value getUserDetailsFromPortal(const size_t userId);

	static constexpr const char* EXTERNAL_ID_PREFIX = "GFBIO:";

	virtual void run();
};

REGISTER_HTTP_SERVICE(GFBioService, "gfbio");

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
					UserDB::createExternalUser(userDetails.get("emailAddress", "").asString(),
											   userDetails.get("firstName", "").asString() + " " + userDetails.get("lastName", "").asString(),
											   userDetails.get("emailAddress", "").asString(), externalId);

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

			pqxx::connection connection (Configuration::get("operators.gbifsource.dbcredentials"));

			connection.prepare("searchSpecies", "SELECT name FROM gbif.gbif_taxon_to_name WHERE lower(name) like lower($1)");
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
			gbif["count"] = (unsigned long long) GFBioDataUtil::countGBIFResults(scientificName);

			sources.append(gbif);

			Json::Value iucn(Json::objectValue);
			iucn["name"] = "IUCN";
			iucn["count"] = (unsigned long long) GFBioDataUtil::countIUCNResults(scientificName);

			sources.append(iucn);

			json["dataSources"] = sources;

			response.sendSuccessJSON(json);
			return;
		}

		if(request == "abcd") {
			auto path = Configuration::get("gfbio.abcd.datapath");

			std::ifstream file(path + "gfbio_datacenters.json");
			if (!file.is_open()) {
				response.sendFailureJSON("gfbio_datacenters.json missing");
				return;
			}

			Json::Reader reader(Json::Features::strictMode());
			Json::Value root;
			if (!reader.parse(file, root)) {
				response.sendFailureJSON("gfbio_datacenters.json invalid");
				return;
			}

			response.sendSuccessJSON(root);
			return;
		}


		//protected methods
		auto session = UserDB::loadSession(params.get("sessiontoken"));

		if (request == "baskets") {

			std::string gfbioId = session->getUser().getExternalid();
			//	if(gfbioId.find(EXTERNAL_ID_PREFIX) != 0)
			//		throw GFBioServiceException("GFBioService: This service is only available for GFBio user.");
			//
			//	gfbioId = gfbioId.substr(strlen(EXTERNAL_ID_PREFIX));
			gfbioId = "12932"; //TODO: remove

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
						entry["title"] = result["title"].asString();
						entry["authors"] = result["authors"].asString();
						entry["dataCenter"] = result["dataCenter"].asString();
						std::string metadataLink = result["metadatalink"].asString();
						entry["metadataLink"] = metadataLink;

						// better way to determine dataCenter that's equally robust?
						if(metadataLink.find("doi.pangaea.de/") != std::string::npos) {
							entry["type"] = "pangaea";
							entry["doi"] = metadataLink.substr(metadataLink.find("doi.pangaea.de/") + strlen("doi.pangaea.de/"));
						} else {
							entry["type"] = "abcd";
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
