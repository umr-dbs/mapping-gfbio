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

/// This class provides methods for user authentication with OpenId Connect
class OpenIdConnectService : public HTTPService {
    public:
        using HTTPService::HTTPService;

        ~OpenIdConnectService() override = default;

        struct OpenIdConnectServiceException
                : public std::runtime_error {
            using std::runtime_error::runtime_error;
        };

        static constexpr const char *EXTERNAL_ID_PREFIX = "OIDC:";

    protected:
        /// Dispatch requests
        auto run() -> void override;

        /// Login using an OpenId Connect access token
        auto login(const std::string &access_token) const -> void;

    private:
        struct User {
            std::string goe_id; // identifier
            std::string email;
            std::string given_name;
            std::string family_name;
            std::string preferred_username;
            std::time_t expiration_time; // unix timestamp in seconds
        };

        /// Download a JSON Web Key Set from a url
        static auto download_jwks(const std::string &url) -> Json::Value;

        /// Converts a JSON Web Key Set into a PEM string
        ///
        /// `n` and `e` must be Base64 encoded
        ///
        /// Taken from https://stackoverflow.com/questions/57217529/how-to-convert-jwk-public-key-to-pem-format-in-c
        ///
        static auto jwks_to_pem(const std::string &n, const std::string &e) -> std::string;

        /// Download user data from an OpenId Connect User Endpoint
        static auto download_user_data(const std::string &url, const std::string &access_token) -> Json::Value;

        /// Create a session for a user
        ///
        /// This method creates a user account as a side effect if the user does not yet exist
        ///
        static auto createSessionAndAccountIfNotExist(const User &user) -> std::string;
};

REGISTER_HTTP_SERVICE(OpenIdConnectService, "oidc"); // NOLINT(cert-err58-cpp)
