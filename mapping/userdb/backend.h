#ifndef USERDB_BACKEND_H
#define USERDB_BACKEND_H

#include "userdb/userdb.h"
#include "util/make_unique.h"


class UserDBBackend {
	friend class UserDB;

	public:
		virtual ~UserDBBackend();

	protected:
		using userid_t = UserDB::userid_t;
		using groupid_t = UserDB::groupid_t;
		using artifactid_t = UserDB::artifactid_t;

		struct UserData {
			userid_t userid;
			std::string username;
			std::string externalid;
			UserDB::Permissions permissions;
			std::vector<groupid_t> groupids;
		};

		struct GroupData {
			groupid_t groupid;
			std::string groupname;
			UserDB::Permissions permissions;
		};

		struct SessionData {
			std::string sessiontoken;
			userid_t userid;
			time_t expires;
		};

		struct ArtifactData {
			artifactid_t artifactid;
			userid_t userid;
			std::string type;
			std::string name;
			time_t lastChanged;
			std::vector<time_t> versions;
		};
		struct ArtifactVersionData {
			time_t timestamp;
			std::string value;
		};



		// Users
		virtual userid_t createUser(const std::string &username, const std::string &realname, const std::string &email, const std::string &password, const std::string &externalid) = 0;
		virtual UserData loadUser(userid_t userid) = 0;
		virtual userid_t loadUserId(const std::string &username) = 0;
		virtual userid_t authenticateUser(const std::string &username, const std::string &password) = 0;
		virtual userid_t findExternalUser(const std::string &externalid) = 0;
		virtual void setUserExternalid(userid_t userid, const std::string &externalid) = 0;
		virtual void setUserPassword(userid_t userid, const std::string &password) = 0;
		virtual void addUserPermission(userid_t userid, const std::string &permission) = 0;
		virtual void removeUserPermission(userid_t userid, const std::string &permission) = 0;

		// Groups
		virtual UserDB::groupid_t createGroup(const std::string &groupname) = 0;
		virtual GroupData loadGroup(groupid_t groupid) = 0;
		virtual groupid_t loadGroupId(const std::string &groupname) = 0;
		virtual void addUserToGroup(userid_t userid, groupid_t groupid) = 0;
		virtual void removeUserFromGroup(userid_t userid, groupid_t groupid) = 0;
		// destroyGroup ? this needs AUTO_INCREMENT to keep groupids unique forever.
		virtual void addGroupPermission(groupid_t groupid, const std::string &permission) = 0;
		virtual void removeGroupPermission(groupid_t groupid, const std::string &permission) = 0;

		// Sessions
		virtual std::string createSession(userid_t, time_t expires) = 0;
		virtual SessionData loadSession(const std::string &sessiontoken) = 0;
		virtual void destroySession(const std::string &sessiontoken) = 0;

		// Artifacts
		virtual artifactid_t createArtifact(userid_t userid, const std::string &type, const std::string &name, const std::string &value) = 0;
		virtual time_t updateArtifactValue(userid_t userid, const std::string &type, const std::string &name, const std::string &value) = 0;
		virtual ArtifactData loadArtifact(artifactid_t artifactid) = 0;
		virtual ArtifactData loadArtifact(const std::string& username, const std::string &type, const std::string &name) = 0;
		virtual ArtifactVersionData loadArtifactVersionData(userid_t userid, artifactid_t artifactid, time_t timestamp) = 0;
		virtual std::vector<ArtifactData> loadArtifactsOfType(userid_t userid, const std::string &type) = 0;
};


class UserDBBackendRegistration {
	public:
		UserDBBackendRegistration(const char *name, std::unique_ptr<UserDBBackend> (*constructor)(const std::string &));
};

#define REGISTER_USERDB_BACKEND(classname, name) static std::unique_ptr<UserDBBackend> create##classname(const std::string &location) { return make_unique<classname>(location); } static UserDBBackendRegistration register_##classname(name, create##classname)


#endif // USERDB_BACKEND_H
