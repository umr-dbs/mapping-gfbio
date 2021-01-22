#pragma once

#include "portal/basketapi.h"
#include "services/httpservice.h"
#include "userdb/userdb.h"
#include "util/concat.h"
#include "util/configuration.h"
#include "util/curl.h"
#include "util/exceptions.h"

#include <cstring>
#include <json/json.h>
#include <algorithm>
#include <pqxx/pqxx>
#include <jwt/jwt.hpp>
#include <cppcodec/base64_url_unpadded.hpp>
#include <util/log.h>

/// This class provides methods for BExIS communication
class BexisService : public HTTPService {
    public:
        using HTTPService::HTTPService;

        ~BexisService() override = default;

        struct BexisServiceException
                : public std::runtime_error {
            using std::runtime_error::runtime_error;
        };

    protected:
        /// Dispatch requests
        auto run() -> void override;

        /// Register an external URL to be callable in MAPPING
        auto register_external_url(const std::string &secretToken, const std::string &url) -> void;

    private:

};

REGISTER_HTTP_SERVICE(BexisService, "bexis"); // NOLINT(cert-err58-cpp)
