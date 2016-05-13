
#include "services/httpservice.h"
#include "userdb/userdb.h"
#include "util/configuration.h"
#include "util/debug.h"

#include "rasterdb/rasterdb.h"

#include <json/json.h>

class UserService : public HTTPService {
	public:
		using HTTPService::HTTPService;
		virtual ~UserService() = default;
		virtual void run();
	private:
		void respond(const Json::Value &result);
		void success(Json::Value &result);
		void success();
		template<typename T>
		void success(const std::string &key, const T& value);
		void failure(const std::string &error);
};
REGISTER_HTTP_SERVICE(UserService, "USER");

/*
 * Return a complete JSON object as response
 */
void UserService::respond(const Json::Value &obj) {
	result.sendContentType("application/json");
	result.sendDebugHeader();
	result.finishHeaders();
	result << obj;

}
/*
 * Return a complete JSON object as successful response
 */
void UserService::success(Json::Value &obj) {
	obj["result"] = true;
	respond(obj);
}

/*
 * Helper: just return success, without any data
 */
void UserService::success() {
	Json::Value obj(Json::ValueType::objectValue);
	success(obj);
}

/*
 * Helper: return a single result value
 */
template<typename T>
void UserService::success(const std::string &key, const T& value) {
	Json::Value obj(Json::ValueType::objectValue);
	obj[key] = value;
	success(obj);
}

/*
 * Return a JSON object indicating failure and an error message
 */
void UserService::failure(const std::string &error) {
	Json::Value obj(Json::ValueType::objectValue);
	obj["result"] = error;
	respond(obj);
}



void UserService::run() {
	try {
		std::string request = params.get("request");

		if (request == "login") {
			auto session = UserDB::createSession(params.get("username"), params.get("password"), 8*3600);
			success("session", session->getSessiontoken());
			return;
		}

		// anything except login is only allowed with a valid session, so check for it.
		auto session = UserDB::loadSession(params.get("sessiontoken"));

		if (request == "logout") {
			session->logout();
			success();
			return;
		}

		if (request == "sourcelist") {
			Json::Value v(Json::ValueType::objectValue);
			auto sourcenames = RasterDB::getSourceNames();
			for (const auto &name : sourcenames) {
				auto description = RasterDB::getSourceDescription(name);
				std::istringstream iss(description);
				Json::Reader reader(Json::Features::strictMode());
				Json::Value root;
				if (!reader.parse(iss, root))
					continue;
				v[name] = root;
			}
			success("sourcelist", v);
			return;
		}

		failure("unknown request");
	}
	catch (const std::exception &e) {
		failure(e.what());
	}
}
