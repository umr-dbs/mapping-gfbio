#include "services/httpservice.h"
#include "userdb/userdb.h"
#include "util/configuration.h"
#include "util/concat.h"
#include "util/gfbiodatautil.h"
#include "portal/basketapi.h"
#include "openid_connect.h"

#include <cstring>
#include <sstream>
#include <json/json.h>
#include <algorithm>
#include <pqxx/pqxx>

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

        ~GFBioService() override = default;

        struct GFBioServiceException
                : public std::runtime_error {
            using std::runtime_error::runtime_error;
        };

    protected:

        /// Dispatch requests
        auto run() -> void override;

    private:

        void search_species();

        void query_data_sources();

        void abcd();

        void pangaeaDataSet();

        void baskets(const std::string &goestern_id);

        void basket(const std::string &goestern_id);
};

REGISTER_HTTP_SERVICE(GFBioService, "gfbio"); // NOLINT(cert-err58-cpp)

void GFBioService::run() {
    try {
        std::string request = params.get("request");

        // PUBLIC METHODS

        // helper methods for gbif source
        if (request == "searchSpecies") return search_species();

        if (request == "queryDataSources") return query_data_sources();

        if (request == "abcd") return abcd();

        if (request == "pangaeaDataSet") return pangaeaDataSet();

        // METHODS REQUIRE LOGIN

        auto session = UserDB::loadSession(params.get("sessiontoken"));

        std::string goestern_id = session->getUser().getExternalid();
        if (goestern_id.find(OpenIdConnectService::EXTERNAL_ID_PREFIX) != 0) { // NOLINT(abseil-string-find-startswith)
            throw GFBioServiceException("GFBioService: This service is only available for GFBio users.");
        }

        goestern_id = goestern_id.substr(strlen(OpenIdConnectService::EXTERNAL_ID_PREFIX));

        if (request == "baskets") return baskets(goestern_id);

        if (request == "basket") return basket(goestern_id);

        response.sendFailureJSON("GFBioService: Invalid request");
    }
    catch (const std::exception &e) {
        response.sendFailureJSON(e.what());
    }
}

void GFBioService::basket(const std::string &goestern_id) {
    size_t basketId = this->params.getLong("id");

    const BasketAPI::Basket &basket = BasketAPI::getBasket(basketId);

    if (concat(basket.goestern_id) != goestern_id) {
        throw GFBioServiceException("Access denied for basket");
    }

    Json::Value json = basket.toJson();
    this->response.sendSuccessJSON(json);
}

void GFBioService::baskets(const std::string &goestern_id) {
    size_t offset = params.getInt("offset", 0);
    size_t limit = params.getInt("limit", 10);

    Json::Value jsonBaskets(Json::arrayValue);

    BasketAPI::BasketsOverview baskets = BasketAPI::getBaskets(goestern_id, offset, limit);
    Json::Value json = baskets.toJson();
    response.sendSuccessJSON(json);
}

void GFBioService::pangaeaDataSet() {
    std::string doi = params.get("doi");

    BasketAPI::PangaeaBasketEntry pangaeaBasket(doi);

    Json::Value json = pangaeaBasket.toJson();
    response.sendSuccessJSON(json);
}

void GFBioService::abcd() {
    Json::Value dataCenters = GFBioDataUtil::getGFBioDataCentersJSON();

    response.sendSuccessJSON(dataCenters);
}

void GFBioService::query_data_sources() {
    std::string term = params.get("term");
    if (term.size() < 3) {
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
    iucn["count"] = (Json::Int) GFBioDataUtil::countIUCNResults(term, level);

    sources.append(iucn);

    json["dataSources"] = sources;

    response.sendSuccessJSON(json);
}

void GFBioService::search_species() {
    std::string term = params.get("term");
    if (term.size() < 3) {
        response.sendFailureJSON("Term has to be >= 3 characters");
        return;
    }

    std::string level = params.get("level");

    pqxx::connection connection(Configuration::get<std::string>("operators.gfbiosource.dbcredentials"));

    connection.prepare("searchSpecies",
                       "SELECT term FROM gbif.taxonomy WHERE term ilike $1 AND level = lower($2) ORDER BY term ASC");
    pqxx::work work(connection);
    pqxx::result result = work.prepared("searchSpecies")(term + "%")(level).exec();

    Json::Value json(Json::objectValue);
    Json::Value names(Json::arrayValue);
    for (const auto row : result) {
        names.append(row[0].as<std::string>());
    }

    json["speciesNames"] = names;
    response.sendSuccessJSON(json);
}
