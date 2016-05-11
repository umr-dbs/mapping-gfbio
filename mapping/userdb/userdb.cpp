
#include "userdb/userdb.h"
#include "userdb/backend.h"

#include "util/exceptions.h"
#include "util/configuration.h"

#include <time.h>
#include <unordered_map>
#include <random>

// all of these just to open /dev/urandom ..
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


/*
 * Permissions
 */
void UserDB::Permissions::addPermission(const std::string &permission) {
	set.insert(permission);
}

void UserDB::Permissions::removePermission(const std::string &permission) {
	set.erase(permission);
}

void UserDB::Permissions::addPermissions(const Permissions &other) {
	for (auto &p : other.set)
		set.insert(p);
}

bool UserDB::Permissions::hasPermission(const std::string &permission) {
	return set.count(permission) == 1;
}



/*
 * User
 */
UserDB::User::User(userid_t userid, const std::string &username, Permissions &&user_permissions, std::vector<std::shared_ptr<Group>> &&groups)
	: userid(userid), username(username), groups(groups), user_permissions(user_permissions) {
	all_permissions.addPermissions(user_permissions);
	for (auto &group : groups)
		all_permissions.addPermissions(group->group_permissions);
}

std::shared_ptr<UserDB::User> UserDB::User::joinGroup(const UserDB::Group &group) {
	UserDB::addUserToGroup(userid, group.groupid);
	return UserDB::loadUser(userid);
}
std::shared_ptr<UserDB::User> UserDB::User::leaveGroup(const UserDB::Group &group) {
	UserDB::removeUserFromGroup(userid, group.groupid);
	return UserDB::loadUser(userid);
}

std::shared_ptr<UserDB::User> UserDB::User::addPermission(const std::string &permission) {
	UserDB::addUserPermission(userid, permission);
	return UserDB::loadUser(userid);
}
std::shared_ptr<UserDB::User> UserDB::User::removePermission(const std::string &permission) {
	UserDB::removeUserPermission(userid, permission);
	return UserDB::loadUser(userid);
}

void UserDB::User::changePassword(const std::string &password) {
	UserDB::changeUserPassword(userid, password);
}

/*
 * Group
 */
UserDB::Group::Group(groupid_t groupid, const std::string &groupname, Permissions &&group_permissions)
	: groupid(groupid), groupname(groupname), group_permissions(group_permissions) {
}

std::shared_ptr<UserDB::Group> UserDB::Group::addPermission(const std::string &permission) {
	UserDB::addGroupPermission(groupid, permission);
	return UserDB::loadGroup(groupid);
}
std::shared_ptr<UserDB::Group> UserDB::Group::removePermission(const std::string &permission) {
	UserDB::removeGroupPermission(groupid, permission);
	return UserDB::loadGroup(groupid);
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

static uint64_t createSeed() {
	uint64_t seed = 0;

	// try the realtime clock first
	struct timespec ts;
	if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
		seed ^= (uint64_t) ts.tv_sec;
		seed ^= (uint64_t) ts.tv_nsec;
	}
	else {
		// if clock_gettime() is not available, fall back to time()
		auto t = time(nullptr);
		if (t != (time_t) -1)
			seed ^= (uint64_t) t;
	}
	// also try the monotonic clock
	if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
		seed ^= (uint64_t) ts.tv_sec;
		seed ^= (uint64_t) ts.tv_nsec;
	}

	// All POSIX sources are exhausted, but many systems have /dev/urandom, so try using that.
	auto f = open("/dev/urandom", O_RDONLY);
	if (f >= 0) {
		uint64_t random = 0;
		if (read(f, &random, sizeof(random)) == sizeof(random)) {
			seed ^= random;
		}
		close(f);
	}

	if (seed == 0)
		throw PlatformException("No usable source of entropy found, cannot seed RNG");

	return seed;
}

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
	rng.seed(createSeed());

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

std::shared_ptr<UserDB::User> UserDB::loadUser(UserDB::userid_t userid) {
	auto userdata = userdb_backend->loadUser(userid);
	std::vector<std::shared_ptr<Group>> groups;
	for (auto groupid : userdata.groupids)
		groups.push_back(loadGroup(groupid));
	auto user = std::make_shared<User>(userid, userdata.username, std::move(userdata.permissions), std::move(groups));
	return user;
}

std::shared_ptr<UserDB::User> UserDB::createUser(const std::string &username, const std::string &password) {
	auto userid = userdb_backend->createUser(username, password);
	return loadUser(userid);
}

void UserDB::addUserPermission(userid_t userid, const std::string &permission) {
	userdb_backend->addUserPermission(userid, permission);
	// TODO: invalidate user cache
}
void UserDB::removeUserPermission(userid_t userid, const std::string &permission) {
	userdb_backend->removeUserPermission(userid, permission);
	// TODO: invalidate user cache
}

void UserDB::changeUserPassword(userid_t userid, const std::string &password) {
	userdb_backend->changeUserPassword(userid, password);
	// TODO: invalidate user cache
}


std::shared_ptr<UserDB::Group> UserDB::loadGroup(UserDB::groupid_t groupid) {
	auto groupdata = userdb_backend->loadGroup(groupid);
	return std::make_shared<Group>(groupid, groupdata.groupname, std::move(groupdata.permissions));
}

std::shared_ptr<UserDB::Group> UserDB::createGroup(const std::string &groupname) {
	auto groupid = userdb_backend->createGroup(groupname);
	return loadGroup(groupid);
}

void UserDB::addGroupPermission(groupid_t groupid, const std::string &permission) {
	userdb_backend->addGroupPermission(groupid, permission);
	// TODO: invalidate group cache
}
void UserDB::removeGroupPermission(groupid_t groupid, const std::string &permission) {
	userdb_backend->removeGroupPermission(groupid, permission);
	// TODO: invalidate group cache
}

void UserDB::addUserToGroup(UserDB::userid_t userid, UserDB::groupid_t groupid) {
	userdb_backend->addUserToGroup(userid, groupid);
	// TODO: invalidate user cache
}
void UserDB::removeUserFromGroup(UserDB::userid_t userid, UserDB::groupid_t groupid) {
	userdb_backend->removeUserFromGroup(userid, groupid);
	// TODO: invalidate user cache
}


std::shared_ptr<UserDB::Session> UserDB::createSession(const std::string &username, const std::string &password, time_t duration_in_seconds) {
	// API keys are normal sessions without an expiration date, modeled by expires = 0.
	time_t expires = 0;
	if (duration_in_seconds > 0)
		expires = time(nullptr) + duration_in_seconds;
	auto userid = userdb_backend->authenticateUser(username, password);
	auto sessiontoken = userdb_backend->createSession(userid, expires);
	return loadSession(sessiontoken);
}

std::shared_ptr<UserDB::Session> UserDB::loadSession(const std::string &sessiontoken) {
	auto sessiondata = userdb_backend->loadSession(sessiontoken);
	auto user = loadUser(sessiondata.userid);
	auto session = std::make_shared<UserDB::Session>(sessiontoken, user, sessiondata.expires);
	if (session->isExpired())
		throw UserDB::session_expired_error();
	return session;
}

void UserDB::destroySession(const std::string &sessiontoken) {
	userdb_backend->destroySession(sessiontoken);
}
