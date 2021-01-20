#include "bexis.h"


auto BexisService::run() -> void {
    try {
        const std::string &request = params.get("request");

        if (request == "register_external_url") {
            this->register_external_url(params.get("token"), params.get("url"));
        } else { // FALLBACK
            response.sendFailureJSON("BexisService: Invalid request");
        }

    } catch (const std::exception &e) {
        response.sendFailureJSON(e.what());
    }
}

auto BexisService::register_external_url(const std::string &secretToken, const std::string &url) -> void {
    static const auto secretTokens = Configuration::getVector<std::string>("bexis.tokens");
    static const auto groupName = Configuration::get<std::string>("bexis.mapping_group_name");

    if (std::find(secretTokens.cbegin(), secretTokens.cend(), secretToken) == secretTokens.cend()) {
        return response.send500("Invalid token");
    }

    const auto group = UserDB::loadGroup(groupName);

    const std::string permission = concat("data.ogr_raw_source.", url);

    if (!group->hasPermission(permission)) {
        group->addPermission(permission);
    }

    response.sendSuccessJSON();
}
