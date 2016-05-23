
#include "userdb/backend.h"
#include "util/configuration.h"
#include "util/sqlite.h"
#include "util/sha1.h"


class SQLiteUserDBBackend : public UserDBBackend {
	friend class UserDB;
	public:
		SQLiteUserDBBackend(const std::string &filename);
		virtual ~SQLiteUserDBBackend();

	protected:
		virtual userid_t createUser(const std::string &username, const std::string &realname, const std::string &email, const std::string &password, const std::string &externalid);
		virtual UserData loadUser(userid_t userid);
		virtual userid_t authenticateUser(const std::string &username, const std::string &password);
		virtual userid_t findExternalUser(const std::string &externalid);
		virtual void setUserExternalid(userid_t userid, const std::string &externalid);
		virtual void setUserPassword(userid_t userid, const std::string &password);
		virtual void addUserPermission(userid_t userid, const std::string &permission);
		virtual void removeUserPermission(userid_t userid, const std::string &permission);

		virtual groupid_t createGroup(const std::string &groupname);
		virtual GroupData loadGroup(groupid_t groupid);
		virtual void addUserToGroup(userid_t userid, groupid_t groupid);
		virtual void removeUserFromGroup(userid_t userid, groupid_t groupid);
		virtual void addGroupPermission(groupid_t groupid, const std::string &permission);
		virtual void removeGroupPermission(groupid_t groupid, const std::string &permission);

		virtual std::string createSession(userid_t, time_t expires);
		virtual SessionData loadSession(const std::string &sessiontoken);
		virtual void destroySession(const std::string &sessiontoken);
	private:
		static std::string createPwdHash(const std::string &password);
		static bool verifyPwdHash(const std::string &password, const std::string &pwhash);

		SQLite db;
};

REGISTER_USERDB_BACKEND(SQLiteUserDBBackend, "sqlite");


SQLiteUserDBBackend::SQLiteUserDBBackend(const std::string &filename) {
	db.open(filename.c_str(), false);

	db.exec("CREATE TABLE IF NOT EXISTS users ("
		" userid INTEGER PRIMARY KEY,"
		" username STRING NOT NULL,"
		" realname STRING NOT NULL,"
		" email STRING NOT NULL,"
		" pwhash STRING NOT NULL,"
		" externalid STRING"
		")"
	);
	db.exec("CREATE UNIQUE INDEX IF NOT EXISTS unique_username ON users(username)");
	// NULL values do not count for the unique index in sqlite
	db.exec("CREATE UNIQUE INDEX IF NOT EXISTS unique_externalid ON users(externalid)");

	db.exec("CREATE TABLE IF NOT EXISTS user_permissions ("
		" userid INTEGER NOT NULL,"
		" permission STRING NOT NULL,"
		" PRIMARY KEY(userid, permission)"
		")"
	);

	db.exec("CREATE TABLE IF NOT EXISTS groups ("
		" groupid INTEGER PRIMARY KEY,"
		" groupname STRING NOT NULL"
		")"
	);
	db.exec("CREATE UNIQUE INDEX IF NOT EXISTS unique_groupname ON groups(groupname)");

	db.exec("CREATE TABLE IF NOT EXISTS group_permissions ("
		" groupid INTEGER NOT NULL,"
		" permission STRING NOT NULL,"
		" PRIMARY KEY(groupid, permission)"
		")"
	);

	db.exec("CREATE TABLE IF NOT EXISTS user_to_group ("
		" userid INTEGER,"
		" groupid INTEGER,"
		" PRIMARY KEY(userid, groupid)"
		")"
	);

	db.exec("CREATE TABLE IF NOT EXISTS sessions ("
		" sessiontoken STRING PRIMARY KEY,"
		" userid INTEGER NOT NULL,"
		" expires INTEGER NOT NULL"
		")"
	);

	// artifacts: artifactid, userid, type, name
	// artifact_versions: artifactid, data, value
}

SQLiteUserDBBackend::~SQLiteUserDBBackend() {
}


/*
 * Users
 */
UserDBBackend::userid_t SQLiteUserDBBackend::createUser(const std::string &username, const std::string &realname, const std::string &email, const std::string &password, const std::string &externalid) {
	auto stmt = db.prepare("INSERT INTO users (username, realname, email, pwhash, externalid) VALUES (?, ?, ?, ?, ?)");
	stmt.bind(1, username);
	stmt.bind(2, realname);
	stmt.bind(3, email);
	if (externalid != "") {
		stmt.bind(4, "external");
		stmt.bind(5, externalid);
	}
	else {
		stmt.bind(4, createPwdHash(password));
		stmt.bind(5, nullptr);
	}
	stmt.exec();
	return db.getLastInsertId();
}

UserDBBackend::UserData SQLiteUserDBBackend::loadUser(userid_t userid) {
	auto stmt = db.prepare("SELECT username FROM users WHERE userid = ?");
	stmt.bind(1, userid);
	if (!stmt.next())
		throw UserDB::database_error("UserDB: user not found");
	std::string username = stmt.getString(0);
	stmt.finalize();

	stmt = db.prepare("SELECT permission FROM user_permissions WHERE userid = ?");
	stmt.bind(1, userid);
	UserDB::Permissions permissions;
	while (stmt.next())
		permissions.addPermission(stmt.getString(0));

	stmt = db.prepare("SELECT groupid FROM user_to_group WHERE userid = ?");
	stmt.bind(1, userid);
	std::vector<groupid_t> groups;
	while (stmt.next())
		groups.push_back(stmt.getInt64(0));

	return UserData{userid, username, std::move(permissions), std::move(groups)};
}

UserDBBackend::userid_t SQLiteUserDBBackend::authenticateUser(const std::string &username, const std::string &password) {
	auto stmt = db.prepare("SELECT userid, pwhash FROM users WHERE username = ?");
	stmt.bind(1, username);
	if (!stmt.next())
		throw UserDB::authentication_error("UserDB: username or password wrong");
	auto userid = stmt.getInt64(0);
	std::string pwhash = stmt.getString(1);
	if (!verifyPwdHash(password, pwhash))
		throw UserDB::authentication_error("UserDB: username or password wrong");

	return userid;
}

UserDBBackend::userid_t SQLiteUserDBBackend::findExternalUser(const std::string &externalid) {
	auto stmt = db.prepare("SELECT userid FROM users WHERE externalid = ?");
	stmt.bind(1, externalid);
	if (!stmt.next())
		throw UserDB::authentication_error("UserDB: username or password wrong");
	auto userid = stmt.getInt64(0);

	return userid;
}

void SQLiteUserDBBackend::setUserExternalid(userid_t userid, const std::string &externalid) {
	auto stmt = db.prepare("UPDATE users SET pwhash = 'external', externalid = ? WHERE userid = ? LIMIT 1");
	stmt.bind(1, externalid);
	stmt.bind(2, userid);
	stmt.exec();
}

void SQLiteUserDBBackend::setUserPassword(userid_t userid, const std::string &password) {
	auto stmt = db.prepare("UPDATE users SET pwhash = ?, externalid = NULL WHERE userid = ? LIMIT 1");
	stmt.bind(1, createPwdHash(password));
	stmt.bind(2, userid);
	stmt.exec();
}

void SQLiteUserDBBackend::addUserPermission(userid_t userid, const std::string &permission) {
	auto stmt = db.prepare("INSERT OR IGNORE INTO user_permissions (userid, permission) VALUES (?, ?)");
	stmt.bind(1, userid);
	stmt.bind(2, permission);
	stmt.exec();
}
void SQLiteUserDBBackend::removeUserPermission(userid_t userid, const std::string &permission) {
	auto stmt = db.prepare("DELETE from user_permissions WHERE userid = ? AND permission = ? LIMIT 1");
	stmt.bind(1, userid);
	stmt.bind(2, permission);
	stmt.exec();
}

/*
 * Groups
 */
UserDBBackend::groupid_t SQLiteUserDBBackend::createGroup(const std::string &groupname) {
	auto stmt = db.prepare("INSERT INTO groups (groupname) VALUES (?)");
	stmt.bind(1, groupname);
	stmt.exec();
	return db.getLastInsertId();
}

UserDBBackend::GroupData SQLiteUserDBBackend::loadGroup(groupid_t groupid) {
	auto stmt = db.prepare("SELECT groupname FROM groups WHERE groupid = ?");
	stmt.bind(1, groupid);
	if (!stmt.next())
		throw UserDB::database_error("UserDB: group not found");
	std::string groupname = stmt.getString(0);

	stmt = db.prepare("SELECT permission FROM group_permissions WHERE groupid = ?");
	stmt.bind(1, groupid);
	UserDB::Permissions permissions;
	while (stmt.next())
		permissions.addPermission(stmt.getString(0));

	return GroupData{groupid, groupname, std::move(permissions)};
}

void SQLiteUserDBBackend::addUserToGroup(userid_t userid, groupid_t groupid) {
	// If the user is already in the group, that's not an error.
	// Sqlite has the INSERT OR IGNORE syntax: http://www.sqlite.org/lang_conflict.html
	auto stmt = db.prepare("INSERT OR IGNORE INTO user_to_group (userid, groupid) VALUES (?, ?)");
	stmt.bind(1, userid);
	stmt.bind(2, groupid);
	stmt.exec();
}
void SQLiteUserDBBackend::removeUserFromGroup(userid_t userid, groupid_t groupid) {
	auto stmt = db.prepare("DELETE from user_to_group WHERE userid = ? AND groupid = ? LIMIT 1");
	stmt.bind(1, userid);
	stmt.bind(2, groupid);
	stmt.exec();
}

void SQLiteUserDBBackend::addGroupPermission(groupid_t groupid, const std::string &permission) {
	auto stmt = db.prepare("INSERT OR IGNORE INTO group_permissions (groupid, permission) VALUES (?, ?)");
	stmt.bind(1, groupid);
	stmt.bind(2, permission);
	stmt.exec();

}
void SQLiteUserDBBackend::removeGroupPermission(groupid_t groupid, const std::string &permission) {
	auto stmt = db.prepare("DELETE from group_permissions WHERE groupid = ? AND permission = ? LIMIT 1");
	stmt.bind(1, groupid);
	stmt.bind(2, permission);
	stmt.exec();
}

/*
 * Sessions
 */
std::string SQLiteUserDBBackend::createSession(userid_t userid, time_t expires) {
	auto sessiontoken = UserDB::createRandomToken(32);
	auto stmt = db.prepare("INSERT INTO sessions (sessiontoken, userid, expires) VALUES (?,?,?)");
	stmt.bind(1, sessiontoken);
	stmt.bind(2, userid);
	stmt.bind(3, expires);
	stmt.exec();

	return sessiontoken;
}

UserDBBackend::SessionData SQLiteUserDBBackend::loadSession(const std::string &sessiontoken) {
	auto stmt = db.prepare("SELECT userid, expires FROM sessions WHERE sessiontoken = ?");
	stmt.bind(1, sessiontoken);
	if (!stmt.next())
		throw UserDB::session_expired_error();
	auto userid = stmt.getInt64(0);
	auto expires = stmt.getInt64(1);

	return SessionData{sessiontoken, userid, expires};
}

void SQLiteUserDBBackend::destroySession(const std::string &sessiontoken) {
	auto stmt = db.prepare("DELETE from sessions WHERE sessiontoken = ?");
	stmt.bind(1, sessiontoken);
	stmt.exec();
}


std::string SQLiteUserDBBackend::createPwdHash(const std::string &password) {
	/*
	 * Hashes are stored in the db as
	 * salt:hash
	 * where salt is randomly generated, and hash is sha1 of password+salt.
	 */
	auto salt = UserDB::createRandomToken(8);
	SHA1 sha1;
	sha1.addBytes(password);
	sha1.addBytes(salt);
	return salt + ":" + sha1.digest().asHex();
}

bool SQLiteUserDBBackend::verifyPwdHash(const std::string &password, const std::string &pwhash) {
	auto colon = pwhash.find(':');
	if (colon == std::string::npos)
		return false;
	auto salt = pwhash.substr(0, colon);
	auto hash = pwhash.substr(colon+1, std::string::npos);
	SHA1 sha1;
	sha1.addBytes(password);
	sha1.addBytes(salt);
	return sha1.digest().asHex() == hash;
}
