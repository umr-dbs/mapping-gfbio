
#include "userdb/backend.h"
#include "util/configuration.h"
#include "util/sqlite.h"


class SQLiteUserDBBackend : public UserDBBackend {
	public:
		SQLiteUserDBBackend(const std::string &filename);
		virtual ~SQLiteUserDBBackend();

		virtual UserDB::userid_t createUser(const std::string &username, const std::string &password);
		virtual std::shared_ptr<UserDB::User> loadUser(UserDB::userid_t userid);

		virtual std::string createSession(const std::string &username, const std::string &password, time_t expires);
		virtual std::shared_ptr<UserDB::Session> loadSession(const std::string &sessiontoken);
		virtual void destroySession(const std::string &sessiontoken);
	private:
		SQLite db;
};

REGISTER_USERDB_BACKEND(SQLiteUserDBBackend, "sqlite");


SQLiteUserDBBackend::SQLiteUserDBBackend(const std::string &filename) {
	db.open(filename.c_str(), false);

	db.exec("CREATE TABLE IF NOT EXISTS users ("
		" userid INTEGER PRIMARY KEY,"
		" username STRING NOT NULL,"
		" pwhash STRING NOT NULL,"
		" groups STRING NOT NULL DEFAULT \"\","
		" permissions STRING NOT NULL DEFAULT \"\""
		")"
	);

	db.exec("CREATE TABLE IF NOT EXISTS groups ("
		" groupid INTEGER PRIMARY KEY,"
		" groupname STRING NOT NULL,"
		" permissions STRING NOT NULL DEFAULT \"\""
		")"
	);

	db.exec("CREATE TABLE IF NOT EXISTS sessions ("
		" sessiontoken STRING PRIMARY KEY,"
		" userid INTEGER NOT NULL,"
		" expires INTEGER NOT NULL"
		")"
	);
}

SQLiteUserDBBackend::~SQLiteUserDBBackend() {
}


UserDB::userid_t SQLiteUserDBBackend::createUser(const std::string &username, const std::string &password) {
	auto stmt = db.prepare("INSERT INTO users (username, pwhash) VALUES (?, ?)");
	stmt.bind(1, username);
	stmt.bind(2, UserDB::createPwdHash(password));
	stmt.exec();
	return db.getLastInsertId();
}

std::shared_ptr<UserDB::User> SQLiteUserDBBackend::loadUser(UserDB::userid_t userid) {
	auto stmt = db.prepare("SELECT username, groups, permissions FROM users WHERE userid = ?");
	stmt.bind(1, userid);
	if (!stmt.next())
		throw UserDB::database_error("UserDB: user not found");
	auto username = stmt.getString(0);
	std::string group_str = stmt.getString(1);
	std::string permission_str = stmt.getString(2);
	stmt.finalize();

	UserDB::Permissions permissions;
	permissions.addUserPermissions(userid, permission_str);
	// iterate over groups
	std::string::size_type pos = 0;
	while (pos != std::string::npos) {
		auto endpos = group_str.find(' ', pos);
		auto g = group_str.substr(pos, endpos);
		if (g.length() > 0) {
			// TODO: load group
			// permissions.addGroupPermissions(group->groupid, permission_str);
		}
		pos = endpos;
	}
	return std::make_shared<UserDB::User>(userid, username, std::move(permissions));
}

std::string SQLiteUserDBBackend::createSession(const std::string &username, const std::string &password, time_t expires) {
	auto stmt = db.prepare("SELECT userid, pwhash, groups FROM users WHERE username = ?");
	stmt.bind(1, username);
	if (!stmt.next())
		throw UserDB::credentials_error();
	auto userid = stmt.getInt64(0);
	std::string pwhash = stmt.getString(1);
	std::string groups = stmt.getString(2);
	if (!UserDB::verifyPwdHash(password, pwhash))
		throw UserDB::credentials_error();
	stmt.finalize();

	auto sessiontoken = UserDB::createRandomToken(32);
	stmt = db.prepare("INSERT INTO sessions (sessiontoken, userid, expires) VALUES (?,?,?)");
	stmt.bind(1, sessiontoken);
	stmt.bind(2, userid);
	stmt.bind(3, expires);
	stmt.exec();

	return sessiontoken;
}

std::shared_ptr<UserDB::Session> SQLiteUserDBBackend::loadSession(const std::string &sessiontoken) {
	auto stmt = db.prepare("SELECT userid, expires FROM sessions WHERE sessiontoken = ?");
	stmt.bind(1, sessiontoken);
	if (!stmt.next())
		throw UserDB::session_expired_error();
	auto userid = stmt.getInt64(0);
	auto expires = stmt.getInt64(1);

	auto user = loadUser(userid);
	auto session = std::make_shared<UserDB::Session>(sessiontoken, user, expires);
	if (session->isExpired())
		throw UserDB::session_expired_error();
	return session;
}

void SQLiteUserDBBackend::destroySession(const std::string &sessiontoken) {
	auto stmt = db.prepare("DELETE from sessions WHERE sessiontoken = ?");
	stmt.bind(1, sessiontoken);
	stmt.exec();
}

