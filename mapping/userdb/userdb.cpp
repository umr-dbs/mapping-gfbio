
#include "userdb/userdb.h"
#include "userdb/backend.h"

#include "util/exceptions.h"
#include "util/configuration.h"
#include "util/concat.h"

#include <time.h>
#include <unordered_map>
#include <random>
#include <cstring>

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
UserDB::User::User(userid_t userid, const std::string &username, const std::string &externalid, Permissions &&user_permissions, std::vector<std::shared_ptr<Group>> &&groups)
	: userid(userid), username(username), externalid(externalid), groups(groups), user_permissions(user_permissions) {
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

void UserDB::User::setExternalid(const std::string &externalid) {
	UserDB::setUserExternalid(userid, externalid);
	this->externalid = externalid;
}
void UserDB::User::setPassword(const std::string &password) {
	UserDB::setUserPassword(userid, password);
	externalid = "";
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

/**
 * Artifact
 */
UserDB::ArtifactVersion::ArtifactVersion(time_t timestamp, const std::string &value)
	: timestamp(timestamp), value(value) {
}

UserDB::Artifact::Artifact(artifactid_t artifactid, std::shared_ptr<User> user, const std::string &type, const std::string &name, time_t lastChanged, std::vector<time_t> versions)
	: artifactid(artifactid), user(user), type(type), name(name), lastChanged(lastChanged), versions(versions) {
}

UserDB::Artifact::Artifact(artifactid_t artifactid, std::shared_ptr<User> user, const std::string &type, const std::string &name, time_t lastChanged)
	: artifactid(artifactid), user(user), type(type), name(name), lastChanged(lastChanged) {
}

std::shared_ptr<UserDB::ArtifactVersion> UserDB::Artifact::getLatestArtifactVersion() {
	return UserDB::loadArtifactVersion(*user, artifactid, time(0));
}

std::shared_ptr<UserDB::ArtifactVersion> UserDB::Artifact::getArtifactVersion(time_t timestamp) {
	return UserDB::loadArtifactVersion(*user, artifactid, timestamp);
}

time_t UserDB::Artifact::updateValue(const std::string &value) {
	auto version = UserDB::updateArtifactValue(*user, type, name, value);
	versions.push_back(version);
	return version;
}

std::shared_ptr<UserDB::User> UserDB::Artifact::shareWithUser(const std::string &username) {
	return UserDB::shareArtifactWithUser(artifactid, username);
}

std::shared_ptr<UserDB::Group> UserDB::Artifact::shareWithGroup(const std::string &groupname) {
	return UserDB::shareArtifactWithGroup(artifactid, groupname);
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
	auto user = std::make_shared<User>(userid, userdata.username, userdata.externalid, std::move(userdata.permissions), std::move(groups));
	return user;
}

std::shared_ptr<UserDB::User> UserDB::createUser(const std::string &username, const std::string &realname, const std::string &email, const std::string &password) {
	auto userid = userdb_backend->createUser(username, realname, email, password, "");
	return loadUser(userid);
}
std::shared_ptr<UserDB::User> UserDB::createExternalUser(const std::string &username, const std::string &realname, const std::string &email, const std::string &externalid) {
	auto userid = userdb_backend->createUser(username, realname, email, "", externalid);
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

void UserDB::setUserPassword(userid_t userid, const std::string &password) {
	userdb_backend->setUserPassword(userid, password);
	// TODO: invalidate user cache
}
void UserDB::setUserExternalid(userid_t userid, const std::string &externalid) {
	userdb_backend->setUserExternalid(userid, externalid);
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

std::shared_ptr<UserDB::Session> UserDB::createSessionForExternalUser(const std::string &externalid, time_t duration_in_seconds) {
	// API keys are normal sessions without an expiration date, modeled by expires = 0.
	time_t expires = 0;
	if (duration_in_seconds > 0)
		expires = time(nullptr) + duration_in_seconds;
	auto userid = userdb_backend->findExternalUser(externalid);
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

std::shared_ptr<UserDB::Artifact> UserDB::createArtifact(const UserDB::User &user, const std::string &type, const std::string &name, const std::string &value) {
	artifactid_t artifactid = userdb_backend->createArtifact(user.userid, type, name, value);
	auto artifactData =  userdb_backend->loadArtifact(artifactid);
	return std::make_shared<Artifact>(artifactid, loadUser(artifactData.userid), artifactData.type, artifactData.name, artifactData.lastChanged, artifactData.versions);
}

std::shared_ptr<UserDB::Artifact> UserDB::loadArtifact(UserDB::User &user, const std::string &username, const std::string &type, const std::string &name) {
	auto artifactData = userdb_backend->loadArtifact(username, type, name);
	if(user.getUsername() == username || user.hasPermission(concat("userdb.artifact.", artifactData.artifactid)))
		return std::make_shared<Artifact>(artifactData.artifactid, loadUser(artifactData.userid), artifactData.type, artifactData.name, artifactData.lastChanged, artifactData.versions);
	throw UserDB::authorization_error("UserDB: Access denied on artifact");
}

std::shared_ptr<UserDB::ArtifactVersion> UserDB::loadArtifactVersion(const User &user, artifactid_t artifactid, time_t timestamp) {
	auto artifactVersionData = userdb_backend->loadArtifactVersionData(user.userid, artifactid, timestamp);
	return std::make_shared<ArtifactVersion>(artifactVersionData.timestamp, artifactVersionData.value);
}

time_t UserDB::updateArtifactValue(const User &user, const std::string &type, const std::string &name, const std::string &value) {
	return userdb_backend->updateArtifactValue(user.userid, type, name, value);
}


std::vector<UserDB::Artifact> UserDB::loadArtifactsOfType(const User &user, const std::string &type) {
	std::vector<UserDB::Artifact> artifacts;

	// resolve accessible artifacts of other users
	for(const std::string &permission : user.all_permissions.set) {
		if(permission.find("userdb.artifact.") == 0){
			size_t artifactid = std::stoi(permission.substr(strlen("userdb.artifact.")));
			auto artifactData = userdb_backend->loadArtifact(artifactid);
			artifacts.push_back(UserDB::Artifact(artifactData.artifactid, loadUser(artifactData.userid), artifactData.type, artifactData.name, artifactData.lastChanged));
		}
	}

	// load user's own artifacts
	auto artifactsData = userdb_backend->loadArtifactsOfType(user.userid, type);

	for(auto& artifactData : artifactsData) {
		artifacts.push_back(UserDB::Artifact(artifactData.artifactid, loadUser(artifactData.userid), artifactData.type, artifactData.name, artifactData.lastChanged));
	}

	return artifacts;
}

std::shared_ptr<UserDB::User> UserDB::shareArtifactWithUser(artifactid_t artifactid, const std::string &username) {
	auto userid = userdb_backend->loadUserId(username);
	auto user = loadUser(userid);
	return user->addPermission(concat("userdb.artifact.", artifactid));
}

std::shared_ptr<UserDB::Group> UserDB::shareArtifactWithGroup(artifactid_t artifactid, const std::string &groupname) {
	auto groupid = userdb_backend->loadGroupId(groupname);
	auto group = loadGroup(groupid);
	return group->addPermission(concat("userdb.artifact.", artifactid));
}
