
#include "userdb/userdb.h"
#include "userdb/backend.h"

#include "util/exceptions.h"
#include "util/configuration.h"
#include "util/sha1.h"

#include <time.h>
#include <memory>
#include <unordered_map>
#include <random>

/*
 * Permissions
 */
void UserDB::Permissions::addUserPermissions(UserDB::userid_t userid, const std::string &permissions) {
	// Every user will by default have the permission user.<userid>
	set.insert(concat("user.", userid));
}
void UserDB::Permissions::addGroupPermissions(UserDB::groupid_t groupid, const std::string &permissions) {
	// all members of a group will by default have the permission group.<groupid>
	set.insert(concat("group.", groupid));
	addPermissionSet(permissions);
}

void UserDB::Permissions::addPermissionSet(const std::string &permissions) {
	std::string::size_type pos = 0;
	while (pos != std::string::npos) {
		auto endpos = permissions.find(' ', pos);
		auto p = permissions.substr(pos, endpos);
		if (p.length() > 0)
			set.insert(p);
		pos = endpos;
	}
}

bool UserDB::Permissions::hasPermission(const std::string &permission) {
	return set.count(permission) == 1;
}


/*
 * User
 */
UserDB::User::User(userid_t userid, const std::string &username, Permissions &&permissions) : userid(userid), username(username), permissions(permissions) {
}

/*
 * Session
 */
UserDB::Session::Session(const std::string &sessiontoken, std::shared_ptr<User> user, time_t expires) : sessiontoken(sessiontoken), user(user), expires(expires) {
}

void UserDB::Session::logout() {
	expires = 1;
	UserDB::destroySession(sessiontoken);
}

bool UserDB::Session::isExpired() {
	if (expires == 0)
		return false;
	if (expires < time(nullptr))
		return true;
	return false;
}


/*
 * UserDB backend registration
 */
static std::unique_ptr<UserDBBackend> userdb_backend;

typedef std::unique_ptr<UserDBBackend> (*BackendConstructor)(const std::string &identifier);

static std::unordered_map< std::string, BackendConstructor > *getRegisteredConstructorsMap() {
	static std::unordered_map< std::string, BackendConstructor > registered_constructors;
	return &registered_constructors;
}

UserDBBackendRegistration::UserDBBackendRegistration(const char *name, BackendConstructor constructor) {
	auto map = getRegisteredConstructorsMap();
	(*map)[std::string(name)] = constructor;
}


UserDBBackend::~UserDBBackend() {
}


/*
 * Randomness
 */
std::mt19937 rng;


/*
 * UserDB
 */
void UserDB::init(const std::string &backend, const std::string &location) {
	if (userdb_backend != nullptr)
		throw MustNotHappenException("UserDB::init() was called multiple times");

	auto map = getRegisteredConstructorsMap();
	if (map->count(backend) != 1)
		throw ArgumentException(concat("Unknown userdb backend: ", backend));

	// seed the rng
	rng.seed(time(nullptr));

	auto constructor = map->at(backend);
	userdb_backend = constructor(location);
}

void UserDB::initFromConfiguration() {
	auto backend = Configuration::get("userdb.backend");
	auto location = Configuration::get("userdb." + backend + ".location");
	init(backend, location);
}

void UserDB::shutdown() {
	userdb_backend = nullptr;
}

std::shared_ptr<UserDB::User> UserDB::createUser(const std::string &username, const std::string &password) {
	auto userid = userdb_backend->createUser(username, password);
	return userdb_backend->loadUser(userid);
}

std::shared_ptr<UserDB::Session> UserDB::createSession(const std::string &username, const std::string &password, time_t duration_in_seconds) {
	// API keys are normal sessions without an expiration date, modeled by expires = 0.
	time_t expires = 0;
	if (duration_in_seconds > 0)
		expires = time(nullptr) + duration_in_seconds;
	// create the session
	auto sessiontoken = userdb_backend->createSession(username, password, expires);
	return userdb_backend->loadSession(sessiontoken);
}

std::shared_ptr<UserDB::Session> UserDB::loadSession(const std::string &sessiontoken) {
	return userdb_backend->loadSession(sessiontoken);
}

void UserDB::destroySession(const std::string &sessiontoken) {
	userdb_backend->destroySession(sessiontoken);
}

/*
 * Helper methods for token generation and hashing
 */
std::string UserDB::createRandomToken(size_t length) {
	static std::string letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	std::uniform_int_distribution<int> distribution(0, letters.length()-1);

	std::string result;
	result.reserve(length);
	for (size_t i = 0; i < length; i++) {
		result += letters[distribution(rng)];
	}
	return result;
}

std::string UserDB::createPwdHash(const std::string &password) {
	/*
	 * Hashes are stored in the backend as
	 * salt:hash
	 * where salt is randomly generated, and hash is the concatenation of password and salt.
	 */
	auto salt = createRandomToken(8);
	SHA1 sha1;
	sha1.addBytes(password);
	sha1.addBytes(salt);
	return salt + ":" + sha1.digest().asHex();
}

bool UserDB::verifyPwdHash(const std::string &password, const std::string &pwhash) {
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
