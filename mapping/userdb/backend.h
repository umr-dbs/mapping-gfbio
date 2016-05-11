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

		struct UserData {
			userid_t userid;
			std::string username;
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

		// Users
		virtual UserDB::userid_t createUser(const std::string &username, const std::string &password) = 0;
		virtual UserData loadUser(userid_t userid) = 0;
		virtual UserDB::userid_t authenticateUser(const std::string &username, const std::string &password) = 0;
		virtual void changeUserPassword(userid_t userid, const std::string &password) = 0;
		virtual void addUserPermission(userid_t userid, const std::string &permission) = 0;
		virtual void removeUserPermission(userid_t userid, const std::string &permission) = 0;

		// Groups
		virtual UserDB::groupid_t createGroup(const std::string &groupname) = 0;
		virtual GroupData loadGroup(groupid_t groupid) = 0;
		virtual void addUserToGroup(userid_t userid, groupid_t groupid) = 0;
		virtual void removeUserFromGroup(userid_t userid, groupid_t groupid) = 0;
		// destroyGroup ? this needs AUTO_INCREMENT to keep groupids unique forever.
		virtual void addGroupPermission(groupid_t groupid, const std::string &permission) = 0;
		virtual void removeGroupPermission(groupid_t groupid, const std::string &permission) = 0;

		// Sessions
		virtual std::string createSession(userid_t, time_t expires) = 0;
		virtual SessionData loadSession(const std::string &sessiontoken) = 0;
		virtual void destroySession(const std::string &sessiontoken) = 0;
};


class UserDBBackendRegistration {
	public:
		UserDBBackendRegistration(const char *name, std::unique_ptr<UserDBBackend> (*constructor)(const std::string &));
};

#define REGISTER_USERDB_BACKEND(classname, name) static std::unique_ptr<UserDBBackend> create##classname(const std::string &location) { return make_unique<classname>(location); } static UserDBBackendRegistration register_##classname(name, create##classname)


#endif // USERDB_BACKEND_H
