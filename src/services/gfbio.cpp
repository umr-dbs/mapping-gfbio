
#include "services/httpservice.h"
#include "userdb/userdb.h"
#include "util/configuration.h"
#include "util/concat.h"
#include "util/exceptions.h"
#include "util/curl.h"
#include "util/gfbiodatautil.h"
#include "portal/basketapi.h"

#include <cstring>
#include <sstream>
#include <json/json.h>
#include <algorithm>
#include <pqxx/pqxx>
#include <util/pangaeaapi.h>

/*
 * This class provides methods for GFBio users
 *
 * Operations:
 * - request = login: login using gfbio portal token
 *   - parameters:
 *     - token
 * - request = baskets: get baskets (overview) from portal
 *   - parameters:
 *     - offset: the first basket to retrieve
 *     - limit: the number of baskets to retrieve
 * - request = basket: get a specific basket from portal
 *   - parameters:
 *     - id: the id of the basket
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
	curl.setOpt(CURLOPT_PROXY, Configuration::get<std::string>("proxy", "").c_str());
	curl.setOpt(CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
	curl.setOpt(CURLOPT_USERPWD, concat(Configuration::get<std::string>("gfbio.portal.user"), ":", Configuration::get<std::string>("gfbio.portal.password")).c_str());
	curl.setOpt(CURLOPT_URL, (Configuration::get<std::string>("gfbio.portal.authenticateurl") + "/token/" + token).c_str());

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
	curl.setOpt(CURLOPT_PROXY, Configuration::get<std::string>("proxy", "").c_str());
	curl.setOpt(CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
	curl.setOpt(CURLOPT_USERPWD, concat(Configuration::get<std::string>("gfbio.portal.user"), ":", Configuration::get<std::string>("gfbio.portal.password")).c_str());
	curl.setOpt(CURLOPT_URL, (concat(Configuration::get<std::string>("gfbio.portal.userdetailswebserviceurl"), "?userId=", userId)).c_str());
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

			std::string level = params.get("level");

			pqxx::connection connection (Configuration::get<std::string>("operators.gfbiosource.dbcredentials"));

			connection.prepare("searchSpecies", "SELECT term FROM gbif.taxonomy WHERE term ilike $1 AND level = lower($2) ORDER BY term ASC");
			pqxx::work work(connection);
			pqxx::result result = work.prepared("searchSpecies")(term + "%")(level).exec();

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
			std::string term = params.get("term");
			if(term.size() < 3) {
				response.sendFailureJSON("Term has to be >= 3 characters");
				return;
			}

			std::string level = params.get("level");

			Json::Value json(Json::objectValue);
			Json::Value sources(Json::arrayValue);

			Json::Value gbif(Json::objectValue);
			gbif["name"] = "GBIF";
			gbif["count"] = (Json::Int) GFBioDataUtil::countGBIFResults(term, level);

			sources.append(gbif);

			Json::Value iucn(Json::objectValue);
			iucn["name"] = "IUCN";
			iucn["count"] = (Json::Int) GFBioDataUtil::countIUCNResults(term ,level);

			sources.append(iucn);

			json["dataSources"] = sources;

			response.sendSuccessJSON(json);
			return;
		}

		if(request == "abcd") {
			Json::Value dataCenters = GFBioDataUtil::getGFBioDataCentersJSON();

			response.sendSuccessJSON(dataCenters);
			return;
		}

        if(request == "pangaeaDataSet") {
            std::string doi = params.get("doi");

            BasketAPI::PangaeaBasketEntry pangaeaBasket(doi);

            Json::Value json = pangaeaBasket.toJson();
            response.sendSuccessJSON(json);

			return;
        }


		//protected methods
		auto session = UserDB::loadSession(params.get("sessiontoken"));

		std::string gfbioId = session->getUser().getExternalid();
		if(gfbioId.find(EXTERNAL_ID_PREFIX) != 0)
			throw GFBioServiceException("GFBioService: This service is only available for GFBio user.");

		gfbioId = gfbioId.substr(strlen(EXTERNAL_ID_PREFIX));

		if (request == "baskets") {
            size_t offset = params.getInt("offset", 0);
            size_t limit = params.getInt("limit", 10);

			Json::Value jsonBaskets(Json::arrayValue);

			BasketAPI::BasketsOverview baskets = BasketAPI::getBaskets(gfbioId, offset, limit);
            Json::Value json = baskets.toJson();
			response.sendSuccessJSON(json);

			return;
		}

        if (request == "basket") {
            size_t basketId = params.getLong("id");

            const BasketAPI::Basket &basket = BasketAPI::getBasket(basketId);

            if (concat(basket.userId) != gfbioId) {
                throw GFBioServiceException("Access denied for basket");
            }

            Json::Value json = basket.toJson();
            response.sendSuccessJSON(json);
            return;
        }

		response.sendFailureJSON("GFBioService: Invalid request");
	}
	catch (const std::exception &e) {
		response.sendFailureJSON(e.what());
	}
}
