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
    static const auto valid_prefixes = std::vector<std::string> {
        "/vsicurl/http://",
        "/vsicurl/https://",
        "/vsicurl_streaming/http://",
        "/vsicurl_streaming/https://"
    };

    if (std::find(secretTokens.cbegin(), secretTokens.cend(), secretToken) == secretTokens.cend()) {
        return response.sendFailureJSON("Invalid token");
    }

    auto invalid_prefix = true;

    for (const auto &prefix : valid_prefixes) {
        if (url.rfind(prefix, 0) == 0) {
            invalid_prefix = false;
            break;
        }
    }

    if (invalid_prefix) {
        return response.sendFailureJSON("URL must start with `/vsicurl/` or `/vsicurl_streaming/` and then http://` or `https://`");
    }

    const auto group = UserDB::loadGroup(groupName);

    const std::string permission = concat("data.ogr_raw_source.", url);

    if (!group->hasPermission(permission)) {
        group->addPermission(permission);
    }

    response.sendSuccessJSON();
}
