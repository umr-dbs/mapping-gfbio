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

        static constexpr const char *EXTERNAL_ID_PREFIX = "OIDC:";

        /// Download a JSON Web Key Set from a url
        auto download_jwks(const std::string &url) const -> Json::Value;

        /// Converts a JSON Web Key Set into a PEM string
        ///
        /// `n` and `e` must be Base64 encoded
        ///
        /// Taken from https://stackoverflow.com/questions/57217529/how-to-convert-jwk-public-key-to-pem-format-in-c
        ///
        auto jwks_to_pem(const std::string &n, const std::string &e) const -> std::string;

        /// Download user data from an OpenId Connect User Endpoint
        auto download_user_data(const std::string &url, const std::string &access_token) const -> Json::Value;

        /// Create a session for a user
        ///
        /// This method creates a user account as a side effect if the user does not yet exist
        ///
        auto createSessionAndAccountIfNotExist(const User &user) const -> std::string;
};

REGISTER_HTTP_SERVICE(OpenIdConnectService, "oidc"); // NOLINT(cert-err58-cpp)


void OpenIdConnectService::run() {
    try {
        const std::string &request = params.get("request");

        if (request == "login") {
            this->login(params.get("access_token"));
        } else { // FALLBACK
            response.sendFailureJSON("OpenIdConnectService: Invalid request");
        }

    } catch (const std::exception &e) {
        response.sendFailureJSON(e.what());
    }
}

auto OpenIdConnectService::login(const std::string &access_token) const -> void {
    static const std::unordered_set<std::string> allowed_jwt_algorithms{"RS256"};
    static const auto jwks_endpoint_url = Configuration::get<std::string>("oidc.jwks_endpoint");
    static const auto user_endpoint_url = Configuration::get<std::string>("oidc.user_endpoint");
    static const auto allowed_clock_skew_seconds = Configuration::get<uint32_t>("oidc.allowed_clock_skew_seconds");

    const auto jwks = this->download_jwks(jwks_endpoint_url);

    const auto jwt_algorithm = jwks.get("alg", "").asString();

    if (allowed_jwt_algorithms.count(jwt_algorithm) <= 0) {
        throw OpenIdConnectService::OpenIdConnectServiceException(
                concat("OpenIdConnectService: Algorithm ", jwt_algorithm, " is not supported"));
    }

    const std::string pem = this->jwks_to_pem(jwks.get("n", "").asString(), jwks.get("e", "").asString());

    const auto decoded_token = jwt::decode(access_token,
                                           jwt::params::algorithms({jwt_algorithm}),
                                           jwt::params::secret(pem),
                                           jwt::params::leeway(allowed_clock_skew_seconds),
                                           jwt::params::verify(true));

    const auto &payload = decoded_token.payload();
    const uint32_t expiration_time = payload.get_claim_value<uint32_t>(jwt::registered_claims::expiration);

    const auto user_json = this->download_user_data(user_endpoint_url, access_token);

    OpenIdConnectService::User user{
            .goe_id = user_json.get("goe_id", "").asString(),
            .email = user_json.get("email", "").asString(),
            .given_name = user_json.get("given_name", "").asString(),
            .family_name = user_json.get("family_name", "").asString(),
            .preferred_username = user_json.get("preferred_username", "").asString(),
            .expiration_time = expiration_time,
    };

    response.sendSuccessJSON("session", this->createSessionAndAccountIfNotExist(user));
}

auto OpenIdConnectService::jwks_to_pem(const std::string &n, const std::string &e) const -> std::string {
    auto n_decoded = cppcodec::base64_url_unpadded::decode(n);
    auto e_decoded = cppcodec::base64_url_unpadded::decode(e);

    BIGNUM *modul = BN_bin2bn(n_decoded.data(), n_decoded.size(), nullptr);
    BIGNUM *expon = BN_bin2bn(e_decoded.data(), e_decoded.size(), nullptr);

    std::unique_ptr<RSA, std::function<void(RSA *)>> rsa(
            RSA_new(),
            [](RSA *rsa) { RSA_free(rsa); }
    );

    RSA_set0_key(rsa.get(), modul, expon, nullptr);

    std::unique_ptr<BIO, std::function<void(BIO *)>> memory_file(
            BIO_new(BIO_s_mem()),
            [](BIO *bio) {
                BIO_set_close(bio, BIO_CLOSE);
                BIO_free_all(bio);
            }
    );

    PEM_write_bio_RSA_PUBKEY(memory_file.get(), rsa.get());

    BUF_MEM *memory_pointer;

    BIO_get_mem_ptr(memory_file.get(), &memory_pointer);

    return std::string(memory_pointer->data, memory_pointer->length);
}

auto OpenIdConnectService::download_jwks(const std::string &url) const -> Json::Value {
    std::stringstream data;
    cURL curl;
    curl.setOpt(CURLOPT_PROXY, Configuration::get<std::string>("proxy", "").c_str());
    curl.setOpt(CURLOPT_URL, url.c_str());
    curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
    curl.setOpt(CURLOPT_WRITEDATA, &data);
    curl.perform();

    try {
        curl.perform();
    } catch (const cURLException &e) {
        throw OpenIdConnectService::OpenIdConnectServiceException("OpenIdConnectService: JSON Web Key Set service unavailable");
    }

    Json::Reader reader(Json::Features::strictMode());
    Json::Value response;
    if (!reader.parse(data.str(), response)
        || response.empty() || !response.isMember("keys")
        || !response["keys"][0].isMember("n") || !response["keys"][0].isMember("e") || !response["keys"][0].isMember("alg")) {
        Log::error(concat(
                "OpenIdConnectService: JSON Web Key Set is invalid (malformed JSON)",
                '\n',
                data.str()
        ));
        throw OpenIdConnectService::OpenIdConnectServiceException("OpenIdConnectService: JSON Web Key Set is invalid (malformed JSON)");
    }

    // return first key
    return response["keys"][0];
}

auto OpenIdConnectService::download_user_data(const std::string &url, const std::string &access_token) const -> Json::Value {
    std::stringstream data;
    cURL curl;
    curl.setOpt(CURLOPT_PROXY, Configuration::get<std::string>("proxy", "").c_str());
    curl.setOpt(CURLOPT_URL, url.c_str());
    curl.setOpt(CURLOPT_HTTPAUTH, CURLAUTH_BEARER); // NOLINT(hicpp-signed-bitwise)
    curl.setOpt(CURLOPT_XOAUTH2_BEARER, access_token.c_str());
    curl.setOpt(CURLOPT_WRITEFUNCTION, cURL::defaultWriteFunction);
    curl.setOpt(CURLOPT_WRITEDATA, &data);
    curl.perform();

    try {
        curl.perform();
    } catch (const cURLException &e) {
        throw OpenIdConnectService::OpenIdConnectServiceException("OpenIdConnectService: User endpoint unavailable");
    }

    Json::Reader reader(Json::Features::strictMode());
    Json::Value response;

    if (!reader.parse(data.str(), response) || response.empty()
        || !response.isMember("goe_id") || !response.isMember("email")) {
        Log::error(concat(
                "OpenIdConnectService: User data is invalid (malformed JSON)",
                '\n',
                data.str()
        ));
        throw OpenIdConnectService::OpenIdConnectServiceException("OpenIdConnectService: User data is invalid (malformed JSON)");
    }

    return response;
}

auto OpenIdConnectService::createSessionAndAccountIfNotExist(const User &user) const -> std::string {
    std::shared_ptr<UserDB::Session> session;

    const auto user_id = OpenIdConnectService::EXTERNAL_ID_PREFIX + user.goe_id;

    const std::time_t current_time = std::time(nullptr);
    const auto expiration_time_in_seconds = user.expiration_time - current_time;

    try {
        // create session for user if he already exists
        session = UserDB::createSessionForExternalUser(user_id, expiration_time_in_seconds);

        // TODO: think about updating user data like email, etc.
    } catch (const UserDB::authentication_error &e) {
        // user does not exist locally => CREATE
        try {
            auto mapping_user = UserDB::createExternalUser(
                    user.preferred_username,
                    concat(user.given_name, ' ', user.family_name),
                    user.email,
                    user_id
            );

            try {
                auto gfbio_group = UserDB::loadGroup("gfbio");
                mapping_user->joinGroup(*gfbio_group);
            } catch (const UserDB::database_error &) {
                auto gfbio_group = UserDB::createGroup("gfbio");
                mapping_user->joinGroup(*gfbio_group);
            }

            session = UserDB::createSessionForExternalUser(user_id, expiration_time_in_seconds);
        } catch (const std::exception &e) {
            throw OpenIdConnectService::OpenIdConnectServiceException(
                    "OpenIdConnectService: Could not create new user from GFBio Single Sign On.");
        }
    }

    return session->getSessiontoken();
}
