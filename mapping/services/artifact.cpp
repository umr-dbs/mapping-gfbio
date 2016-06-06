
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
 * This class provides access to the artifacts in the UserDB.
 * Parameter request defines the type of request
 *
 * Operations:
 * - request = create: Create a new artifact
 *   - parameters:
 *     - type
 *     - name
 *     - value
 * - request = update: Update the value of an existing artifact
 *   - parameters:
 *     - type
 *     - name
 *     - value
 * - request = get: get the value of a given artifact at a given time (latest version if not specified)
 *   - parameters:
 *     - username
 *     - type
 *     - name
 *     - time (optional)
 * - request = list: list all artifacts of a given type
 *   - parmeters:
 *     - type
 * - request = share: share an artifact with a given user
 *   - parameters:
 *     - username
 *     - type
 *     - name
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
	try {
		std::string request = params.get("request");

		auto session = UserDB::loadSession(params.get("sessiontoken"));
		auto user = session->getUser();

		if(request == "create") {
			std::string type = params.get("type");
			std::string name = params.get("name");
			std::string value = params.get("value");

			user.createArtifact(type, name, value);

			response.sendSuccessJSON();
		} else if(request == "update") {
			std::string type = params.get("type");
			std::string name = params.get("name");
			std::string value = params.get("value");

			auto artifact = user.loadArtifact(user.getUsername(), type, name);
			artifact->updateValue(value);

			response.sendSuccessJSON();
		} else if(request == "get") {
			std::string username = params.get("username");
			std::string type = params.get("type");
			std::string name = params.get("name");

			std::string time = params.get("time", "9999-12-31T23:59:59");
			auto timeParser = TimeParser::create(TimeParser::Format::ISO);

			double timestamp = timeParser->parse(time);

			auto artifact = user.loadArtifact(user.getUsername(), type, name);
			std::string value = artifact->getArtifactVersion(timestamp)->getValue();

			Json::Value json(Json::objectValue);
			json["value"] = value;

			response.sendSuccessJSON(json);
		} else if(request == "list") {
			std::string type = params.get("type");

			auto artifacts = user.loadArtifactsOfType(type);

			Json::Value jsonArtifacts(Json::arrayValue);
			for(auto artifact : artifacts) {
				Json::Value entry(Json::objectValue);
				entry["user"] = artifact.getUser().getUsername();
				entry["type"] = artifact.getType();
				entry["name"] = artifact.getName();

				jsonArtifacts.append(entry);
			}

			Json::Value json(Json::objectValue);
			json["artifacts"] = jsonArtifacts;
			response.sendSuccessJSON(json);
		} else if(request == "share") {
			std::string username = params.get("username");
			std::string type = params.get("type");
			std::string name = params.get("name");

			std::string permission = params.get("permission", "");

			auto artifact = user.loadArtifact(user.getUsername(), type, name);
			if(permission == "user")
				artifact->shareWithUser(permission);
			else if(permission == "group")
				artifact->shareWithGroup(permission);
			else
				throw ArtifactServiceException("ArtifactService: invalid permission target");

			response.sendSuccessJSON();
		}
	}
	catch (const std::exception &e) {
		response.sendFailureJSON(e.what());
	}
}
