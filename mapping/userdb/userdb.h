#ifndef USERDB_USERDB_H
#define USERDB_USERDB_H

#include <string>
#include <unordered_set>
#include <memory>
#include <exception>

class UserDB {
	public:
		// typedefs
		using userid_t = int64_t;
		using groupid_t = int64_t;

		// helper classes
		class Permissions {
			public:
				void addUserPermissions(userid_t userid, const std::string &permissions);
				void addGroupPermissions(groupid_t groupid, const std::string &permissions);
				bool hasPermission(const std::string &permission);
			private:
				void addPermissionSet(const std::string &permissions);
				std::unordered_set<std::string> set;
		};
		class User {
			public:
				User(userid_t userid, const std::string &username, Permissions &&permissions);

				userid_t getUserID() { return userid; }
				const std::string &getUsername() { return username; }
				bool hasPermission(const std::string &permission) { return permissions.hasPermission(permission); }
				// changePassword() ?
			private:
				userid_t userid;
				std::string username;
				// store groups here?
				Permissions permissions;
		};
		class Session {
			public:
				Session(const std::string &sessiontoken, std::shared_ptr<User> user, time_t expires);

				void logout();
				User &getUser() { return *user; };
				const std::string &getSessiontoken() { return sessiontoken; }
				bool isExpired();
			private:
				std::string sessiontoken;
				std::shared_ptr<User> user;
				time_t expires;
		};

		// exceptions
		struct userdb_error
			: public std::runtime_error { using std::runtime_error::runtime_error; };
		struct database_error
			: public userdb_error { using userdb_error::userdb_error; };
		struct credentials_error
			: public userdb_error { credentials_error() : userdb_error("UserDB: username or password is wrong.") {}; };
		struct session_expired_error
			: public userdb_error { session_expired_error() : userdb_error("UserDB: your session has expired, you need to login again.") {}; };

		// static methods
		static void initFromConfiguration();
		static void init(const std::string &backend, const std::string &location);
		static void shutdown();

		static std::shared_ptr<User> createUser(const std::string &username, const std::string &password);
		static std::shared_ptr<Session> createSession(const std::string &username, const std::string &password, time_t duration_in_seconds = 0);
		static std::shared_ptr<Session> loadSession(const std::string &token);

		// these should only be called by backend implementations
		static std::string createRandomToken(size_t length);
		static std::string createPwdHash(const std::string &password);
		static bool verifyPwdHash(const std::string &password, const std::string &pwhash);

	private:
		static void destroySession(const std::string &sessiontoken);

		// TODO: sessioncache?
};


#endif // USERDB_USERDB_H
