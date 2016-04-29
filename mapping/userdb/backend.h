#ifndef USERDB_BACKEND_H
#define USERDB_BACKEND_H

#include "userdb/userdb.h"
#include "util/make_unique.h"


class UserDBBackend {
	public:
		virtual ~UserDBBackend();

		// Users
		virtual UserDB::userid_t createUser(const std::string &username, const std::string &password) = 0;
		virtual std::shared_ptr<UserDB::User> loadUser(UserDB::userid_t userid) = 0;
		// changeUserPassword
		// addUserPermission
		// removeUserPermission
		// addUserToGroup
		// removeUserFromGroup

		// Sessions
		virtual std::string createSession(const std::string &username, const std::string &password, time_t expires) = 0;
		virtual std::shared_ptr<UserDB::Session> loadSession(const std::string &sessiontoken) = 0;
		virtual void destroySession(const std::string &sessiontoken) = 0;

		// Groups
		// createGroup
		// destroyGroup ? this needs AUTO_INCREMENT to keep groupids unique forever.
		// addGroupPermission
		// removeGroupPermission
};


class UserDBBackendRegistration {
	public:
		UserDBBackendRegistration(const char *name, std::unique_ptr<UserDBBackend> (*constructor)(const std::string &));
};

#define REGISTER_USERDB_BACKEND(classname, name) static std::unique_ptr<UserDBBackend> create##classname(const std::string &location) { return make_unique<classname>(location); } static UserDBBackendRegistration register_##classname(name, create##classname)


#endif // USERDB_BACKEND_H
