
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
};
REGISTER_HTTP_SERVICE(UserService, "USER");



void UserService::run() {
	try {
		std::string request = params.get("request");

		if (request == "login") {
			auto session = UserDB::createSession(params.get("username"), params.get("password"), 8*3600);
			response.sendSuccessJSON("session", session->getSessiontoken());
			return;
		}

		// anything except login is only allowed with a valid session, so check for it.
		auto session = UserDB::loadSession(params.get("sessiontoken"));

		if (request == "logout") {
			session->logout();
			response.sendSuccessJSON();
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
			response.sendSuccessJSON("sourcelist", v);
			return;
		}

		response.sendFailureJSON("unknown request");
	}
	catch (const std::exception &e) {
		response.sendFailureJSON(e.what());
	}
}
