
#include "services/httpservice.h"
#include "userdb/userdb.h"
#include "util/configuration.h"
#include "util/concat.h"
#include "util/exceptions.h"
#include "util/curl.h"
#include "util/timeparser.h"

#include <cstring>
#include <sstream>
#include <json/json.h>

/*
 * This class provides access to the artifacts in the UserDB
 */
class ArtifactService : public HTTPService {
public:
	using HTTPService::HTTPService;
	virtual ~ArtifactService() = default;

	struct ArtifactServiceException
		: public std::runtime_error { using std::runtime_error::runtime_error; };

private:
	virtual void run();
};

REGISTER_HTTP_SERVICE(ArtifactService, "artifact");


void ArtifactService::run() {
	std::string request = params.get("request");

	auto session = UserDB::loadSession(params.get("sessiontoken"));
	auto user = session->getUser();

	if(request == "create") {
		std::string type = params.get("type");
		std::string name = params.get("name");
		std::string value = params.get("value");

		UserDB::createArtifact(user, type, name, value);

		response.sendContentType("application/json");
		response.finishHeaders();
		response << "{}";
	} else if(request == "update") {
		std::string type = params.get("type");
		std::string name = params.get("name");
		std::string value = params.get("value");

		auto artifact = UserDB::loadArtifact(user, user.getUsername(), type, name);
		artifact->updateValue(value);

		response.sendContentType("application/json");
		response.finishHeaders();
		response << "{}";
	} else if(request == "get") {
		std::string username = params.get("username");
		std::string type = params.get("type");
		std::string name = params.get("name");

		std::string time = params.get("time", "9999-12-31T23:59:59");
		auto timeParser = TimeParser::create(TimeParser::Format::ISO);

		double timestamp = timeParser->parse(time);

		auto artifact = UserDB::loadArtifact(user, user.getUsername(), type, name);
		std::string value = artifact->getArtifactVersion(timestamp)->getValue();

		Json::Value json(Json::objectValue);
		json["value"] = value;

		response.sendContentType("application/json");
		response.finishHeaders();
		response << json;
	} else if(request == "list") {
		std::string type = params.get("type");

		auto artifacts = UserDB::loadArtifactsOfType(user, type);

		Json::Value json(Json::arrayValue);
		for(auto artifact : artifacts) {
			Json::Value entry(Json::objectValue);
			entry["user"] = artifact.getUser().getUsername();
			entry["type"] = artifact.getType();
			entry["name"] = artifact.getName();

			json.append(entry);
		}

		response.sendContentType("application/json");
		response.finishHeaders();
		response << json;
	} else if(request == "share") {
		std::string username = params.get("username");
		std::string type = params.get("type");
		std::string name = params.get("name");

		std::string permission = params.get("permission", "");

		auto artifact = UserDB::loadArtifact(user, user.getUsername(), type, name);
		if(permission == "user")
			artifact->shareWithUser(permission);
		else if(permission == "group")
			artifact->shareWithGroup(permission);
		else
			throw ArtifactServiceException("ArtifactService: invalid permission target");

		response.sendContentType("application/json");
		response.finishHeaders();
		response << "{}";
	}
}
