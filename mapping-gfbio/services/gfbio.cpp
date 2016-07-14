
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

#include <dirent.h>

#include <pqxx/pqxx>

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
	std::string authenticateWithPortal(const std::string &token);
	Json::Value getUserDetailsFromPortal(const std::string &userId);

	static constexpr const char* EXTERNAL_ID_PREFIX = "GFBIO:";

	virtual void run();
};

REGISTER_HTTP_SERVICE(GFBioService, "gfbio");

/**
 * authenticate user token with portal
 * @return portaluserId of the user
 */
std::string GFBioService::authenticateWithPortal(const std::string &token) {
	std::ostringstream data;
	cURL curl;
	curl.setOpt(CURLOPT_URL, Configuration::get("userdb.gfbio.authenticationurl").c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
	curl.setOpt(CURLOPT_WRITEDATA, &data);
	//TODO: token as parameter

	try {
		curl.perform();
	} catch (const cURLException& e) {
		throw GFBioService::GFBioServiceException("GFBioService: Portal unavailable");
	}

	Json::Reader reader(Json::Features::strictMode());
	Json::Value response;
	if (!reader.parse(data.str(), response))
		throw GFBioService::GFBioServiceException("GFBioService: Portal response invalid (malformed JSON)");

	if(response.get("success", false).asBool()) {
		if(!response.isMember("userId"))
			throw GFBioService::GFBioServiceException("GFBioService: Portal response invalid (missing userId)");

		return response["userId"].asString();
	} else
		throw GFBioService::GFBioServiceException("GFBioService: wrong portal credentials");
}

/**
 * get user details from portal for given userId
 */
Json::Value GFBioService::getUserDetailsFromPortal(const std::string &userId) {
	std::ostringstream data;
	cURL curl;
	curl.setOpt(CURLOPT_URL, Configuration::get("gfbio.portal.userdetailswebserviceurl").c_str());
	curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
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

	return response;
}

void GFBioService::run() {
	try {
		std::string request = params.get("request");

		if(request == "login"){
			std::string token = params.get("token");

			std::string gfbioId = authenticateWithPortal(token);
			std::string externalId = EXTERNAL_ID_PREFIX + gfbioId;

			std::shared_ptr<UserDB::Session> session;
			try {
				session = UserDB::createSessionForExternalUser(externalId, 8 * 3600);
			} catch (const UserDB::authentication_error&) {
				//user does not exist locally => create
				Json::Value userDetails = getUserDetailsFromPortal(externalId);
				UserDB::createExternalUser(userDetails.get("email", "").asString(),
										   userDetails.get("name", "").asString(),
										   userDetails.get("email", "").asString(), externalId);

				session = UserDB::createSessionForExternalUser(externalId, 8 * 3600);
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
			curl.setOpt(CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
			std::string auth = Configuration::get("gfbio.portal.user") + ":" + Configuration::get("gfbio.portal.password");
			curl.setOpt(CURLOPT_USERPWD, concat(Configuration::get("gfbio.portal.user"), ":", Configuration::get("gfbio.portal.password")).c_str());
			curl.setOpt(CURLOPT_URL, concat(Configuration::get("gfbio.portal.basketwebserviceurl"), "?userId=", gfbioId).c_str());
			curl.setOpt(CURLOPT_PROXY, Configuration::get("proxy").c_str());
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

			Json::Value json(Json::arrayValue);
			json["baskets"] = jsonBaskets;
			response.sendSuccessJSON(json);

			return;
		}

		if(request == "abcd") {
			auto path = Configuration::get("gfbio.abcd.datapath");

			std::string suffix = ".xml";

			DIR *dir = opendir(path.c_str());
			struct dirent *ent;
			if (dir == nullptr)
				throw ArgumentException(concat("Could not open path for enumerating: ", path));

			Json::Value json(Json::objectValue);
			Json::Value archives(Json::arrayValue);

			while ((ent = readdir(dir)) != nullptr) {
				std::string name = ent->d_name;
				if (name.length() > suffix.length() && name.compare(name.length() - suffix.length(), suffix.length(), suffix) == 0) {
					// TODO store and get metadata
					Json::Value archive(Json::objectValue);
					archive["file"] = name;
					archive["provider"] = name.substr(0, name.find("_"));
					archive["dataset"] = name.substr(name.find("_") + 1, name.length() - name.find("_") - 5);
					archive["link"] = "http://example.com";
					archives.append(archive);
				}
			}
			closedir(dir);

			json["archives"] = archives;
			response.sendSuccessJSON(json);
			return;
		}

	}
	catch (const std::exception &e) {
		response.sendFailureJSON(e.what());
	}
}
