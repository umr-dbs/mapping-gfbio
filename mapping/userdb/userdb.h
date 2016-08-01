#ifndef USERDB_USERDB_H
#define USERDB_USERDB_H

#include <string>
#include <unordered_set>
#include <memory>
#include <exception>
#include <vector>


/*
 * This is a module for handling users and associated data. A user can belong to several groups. Both users and groups have permissions.
 * A user can create a session for authentification.
 * TODO: artifact-handling
 *
 * The UserDB will always return shared_ptr's to immutable objects. Returning shared mutable objects would not be thread-safe; returning
 * copies everywhere is slow.
 * As a consequence, every method call that would change an object will return a shared_ptr to a new, immutable object.
 *
 * TODO: cache objects
 */
class UserDB {
	protected:
		// these are for internal use only, they should never be leaked outside of the userdb
		using userid_t = int64_t;
		using groupid_t = int64_t;
		using artifactid_t = int64_t;
		friend class UserDBBackend;

	public:
		class Permissions;
		class User;
		class Group;
		class Session;
		class Artifact;

		// helper classes
		class Cacheable {
			public:
				Cacheable();
				time_t cache_expires;
		};

		class Permissions {
			friend class UserDB;
			public:
				Permissions() = default;
				void addPermission(const std::string &permission);
				void removePermission(const std::string &permission);
				void addPermissions(const Permissions &other);
				bool hasPermission(const std::string &permission);
			private:
				std::unordered_set<std::string> set;
		};
		class User {
			friend class UserDB;

			public:
				User(userid_t userid, const std::string &username, const std::string &realname, const std::string &email, const std::string &externalid, Permissions &&user_permissions, std::vector<std::shared_ptr<Group>> &&groups);

				const std::string &getUsername() { return username; }
				const std::string &getRealname() { return realname; }
				const std::string &getEmail() { return email; }
				const std::string &getExternalid() { return externalid; }
				bool hasPermission(const std::string &permission) { return all_permissions.hasPermission(permission); }
				std::shared_ptr<User> joinGroup(const UserDB::Group &group);
				std::shared_ptr<User> leaveGroup(const UserDB::Group &group);
				void setPassword(const std::string &password);
				void setExternalid(const std::string &externalid);
				std::shared_ptr<User> addPermission(const std::string &permission);
				std::shared_ptr<User> removePermission(const std::string &permission);

				/**
				 * Create an artifact
				 * @param type the type of the artifact
				 * @param name the name of the artifact
				 * @param value the value of the artifact
				 * @return the new artifact
				 */
				std::shared_ptr<UserDB::Artifact> createArtifact(const std::string &type, const std::string &name, const std::string &value);

				/**
				 * Load latest version of an artifact.
				 * If no artifact satisfying the parameters exists, an exception is thrown.
				 * If the user has no permission to load the artifact an exception is thrown (TODO)
				 * @param username name of the user who owns the artifact
				 * @param type the type of the artifact
				 * @param name the name of the artifact
				 * @return the artifact
				 */
				std::shared_ptr<Artifact> loadArtifact(const std::string &username, const std::string &type, const std::string &name);

				/**
				 * Load a list of the names of all artifacts a given user has permission to on of a given type
				 * @param type the type of the artifacts
				 * @return the artifact
				 */
				std::vector<Artifact> loadArtifactsOfType(const std::string &type);



			private:
				userid_t userid;
				std::string username;
				std::string realname;
				std::string email;
				std::string externalid;
				std::vector<std::shared_ptr<Group>> groups;
				Permissions user_permissions;
				Permissions all_permissions;
		};
		class Group {
			public:
				Group(groupid_t groupid, const std::string &groupname, Permissions &&group_permissions);

				const std::string &getGroupname() { return groupname; }
				bool hasPermission(const std::string &permission) { return group_permissions.hasPermission(permission); }
				std::shared_ptr<Group> addPermission(const std::string &permission);
				std::shared_ptr<Group> removePermission(const std::string &permission);
			private:
				groupid_t groupid;
				std::string groupname;
				Permissions group_permissions;
				friend class User;
		};
		class Session : public Cacheable {
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
		class ArtifactVersion {
			public:
				ArtifactVersion(time_t timestamp, const std::string &value);
				time_t getTimestamp() { return timestamp; };
				std::string &getValue() { return value; };
			private:
				time_t timestamp;
				std::string value;
		};
		class Artifact {
			public:
				Artifact(artifactid_t artifactid, std::shared_ptr<User> user, const std::string &type, const std::string &name, time_t lastChanged, std::vector<time_t> versions);
				Artifact(artifactid_t artifactid, std::shared_ptr<User> user, const std::string &type, const std::string &name, time_t lastChanged);
				std::string &getType() { return type; };
				std::string &getName() { return name; };
				User &getUser() { return *user; };
				time_t getLastChanged() { return lastChanged; };
				std::shared_ptr<ArtifactVersion> getLatestArtifactVersion();
				std::shared_ptr<ArtifactVersion> getArtifactVersion(time_t timestamp);
				std::vector<time_t> &getVersions() { return versions; }
				time_t updateValue(const std::string &value);
				std::shared_ptr<UserDB::User> shareWithUser(const std::string& username);
				std::shared_ptr<UserDB::Group> shareWithGroup(const std::string& groupname);
			private:
				artifactid_t artifactid;
				std::shared_ptr<User> user;
				std::vector<time_t> versions;
				std::string username;
				std::string type;
				std::string name;
				time_t lastChanged;
		};
		class Clock {
			public:
				virtual ~Clock();
				virtual time_t time() = 0;
		};

		// exceptions
		struct userdb_error
			: public std::runtime_error { using std::runtime_error::runtime_error; };
		struct database_error
			: public userdb_error { using userdb_error::userdb_error; };
		struct authentication_error
			: public userdb_error { using userdb_error::userdb_error; };
		struct authorization_error
			: public userdb_error { using userdb_error::userdb_error; };
		struct session_expired_error
			: public userdb_error { session_expired_error() : userdb_error("UserDB: your session has expired, you need to login again.") {}; };
		struct artifact_error
			: public userdb_error { using userdb_error::userdb_error; };

		// static methods
		static void initFromConfiguration();
		static void init(const std::string &backend, const std::string &location, std::unique_ptr<Clock> clock = nullptr, int sessioncache_timeout = 0);
		static void shutdown();

		static std::shared_ptr<User> createUser(const std::string &username, const std::string &realname, const std::string &email, const std::string &password);
		static std::shared_ptr<User> createExternalUser(const std::string &username, const std::string &realname, const std::string &email, const std::string &externalid);
		static std::shared_ptr<Session> createSession(const std::string &username, const std::string &password, time_t duration_in_seconds = 0);
		static std::shared_ptr<Session> createSessionForExternalUser(const std::string &externalid, time_t duration_in_seconds = 0);
		static std::shared_ptr<Session> loadSession(const std::string &token);

		static std::shared_ptr<Group> createGroup(const std::string &groupname);

		// these should only be called by backend implementations
		static std::string createRandomToken(size_t length);
	private:
		static std::shared_ptr<User> loadUser(userid_t userid);
		static void addUserPermission(userid_t userid, const std::string &permission);
		static void removeUserPermission(userid_t userid, const std::string &permission);
		static void setUserPassword(userid_t userid, const std::string &password);
		static void setUserExternalid(userid_t userid, const std::string &externalid);

		static std::shared_ptr<Group> loadGroup(groupid_t groupid);
		static void addGroupPermission(groupid_t groupid, const std::string &permission);
		static void removeGroupPermission(groupid_t groupid, const std::string &permission);
		static void addUserToGroup(userid_t userid, groupid_t groupid);
		static void removeUserFromGroup(userid_t userid, groupid_t groupid);

		static void destroySession(const std::string &sessiontoken);

		/**
		 * Create an artifact
		 * @param user the user who creates this artifact
		 * @param type the type of the artifact
		 * @param name the name of the artifact
		 * @param value the value of the artifact
		 * @return the new artifact
		 */
		static std::shared_ptr<Artifact> createArtifact(const User &user, const std::string &type, const std::string &name, const std::string &value);

		/**
		 * Load latest version of an artifact.
		 * If no artifact satisfying the parameters exists, an exception is thrown.
		 * If the user has no permission to load the artifact an exception is thrown (TODO)
		 * @param user the user who wants to load the artifact
		 * @param username name of the user who owns the artifact
		 * @param type the type of the artifact
		 * @param name the name of the artifact
		 * @return the artifact
		 */
		static std::shared_ptr<Artifact> loadArtifact(User &user, const std::string &username, const std::string &type, const std::string &name);

		/**
		 * Load a list of the names of all artifacts a given user has permission to on of a given type
		 * @param user the user who wants to load the artifacts
		 * @param type the type of the artifacts
		 * @return the artifact
		 */
		static std::vector<Artifact> loadArtifactsOfType(const User &user, const std::string &type);

		static std::shared_ptr<ArtifactVersion> loadArtifactVersion(const User &user, artifactid_t artifactid, time_t timestamp);
		static time_t updateArtifactValue(const User &user, const std::string &type, const std::string &name, const std::string &value);
		static std::shared_ptr<UserDB::User> shareArtifactWithUser(artifactid_t artifactid, const std::string &username);
		static std::shared_ptr<UserDB::Group> shareArtifactWithGroup(artifactid_t artifactid, const std::string &username);

		static time_t time();
		static std::unique_ptr<Clock> clock;
};


#endif // USERDB_USERDB_H
