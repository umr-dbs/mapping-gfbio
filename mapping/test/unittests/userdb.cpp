#include "userdb/userdb.h"
#include "util/configuration.h"
#include "util/exceptions.h"
#include "util/make_unique.h"

#include <gtest/gtest.h>

class UserDBTestClock : public UserDB::Clock {
	public:
		UserDBTestClock(time_t *now) : now(now) {}
		virtual ~UserDBTestClock() {};
		virtual time_t time() {
			return *now;
		};
		time_t *now;
};

TEST(UserDB, testALL) {
	// We must protect against overwriting the production DB, so make sure to use a custom configuration here!
	// init() will throw if the UserDB was initialized before.
	time_t now = time(nullptr);
	UserDB::init("sqlite", ":memory:", make_unique<UserDBTestClock>(&now));

	const std::string username = "dummy";
	const std::string password = "12345";
	const std::string password2 = "luggage";
	const std::string externalid = "externalsystem:42";
	const std::string groupname = "mygroup";
	const std::string userpermission = "user_can_do_stuff";
	const std::string grouppermission = "group_members_can_do_stuff";

	// Create a user
	auto user = UserDB::createUser(username, "realname", "email", password);
	EXPECT_EQ(user->getUsername(), username);

	// Test user permissions
	EXPECT_EQ(false, user->hasPermission(userpermission));
	user = user->addPermission(userpermission);
	EXPECT_EQ(true, user->hasPermission(userpermission));
	user = user->removePermission(userpermission);
	EXPECT_EQ(false, user->hasPermission(userpermission));

	// create sessions
	EXPECT_THROW(UserDB::createSession(username, "wrong password"), UserDB::authentication_error);

	auto session = UserDB::createSession(username, password);
	EXPECT_EQ(session->getUser().getUsername(), username);

	auto session2 = UserDB::createSession(username, password);
	EXPECT_EQ(session2->getUser().getUsername(), username);
	EXPECT_NE(session->getSessiontoken(), session2->getSessiontoken());

	// load sessions
	EXPECT_THROW(UserDB::loadSession("wrong token"), UserDB::session_expired_error);

	session = UserDB::loadSession(session->getSessiontoken());
	EXPECT_EQ(session->getUser().getUsername(), username);

	// destroy session
	auto sessiontoken = session->getSessiontoken();
	session->logout();

	EXPECT_THROW(UserDB::loadSession(sessiontoken), UserDB::session_expired_error);

	// change password, try logging in again
	user->setPassword(password2);
	EXPECT_THROW(UserDB::createSession(username, password), UserDB::authentication_error);
	EXPECT_NO_THROW(UserDB::createSession(username, password2));

	// mark user as an external user, having no own password
	user->setExternalid(externalid);
	EXPECT_THROW(UserDB::createSession(username, password2), UserDB::authentication_error);
	EXPECT_NO_THROW(UserDB::createSessionForExternalUser(externalid));

	// create a group
	auto group = UserDB::createGroup(groupname);
	EXPECT_EQ(group->getGroupname(), groupname);

	// add permissions
	group = group->addPermission(grouppermission);
	group = group->addPermission(userpermission);
	EXPECT_EQ(true, group->hasPermission(grouppermission));
	EXPECT_EQ(true, group->hasPermission(userpermission));
	group = group->removePermission(userpermission);
	EXPECT_EQ(true, group->hasPermission(grouppermission));
	EXPECT_EQ(false, group->hasPermission(userpermission));

	// add a user, see if our user inherited the permission
	EXPECT_EQ(false, user->hasPermission(grouppermission));
	auto user2 = user->joinGroup(*group);
	// the user object is immutable, so the old one is still without the permission
	EXPECT_EQ(false, user->hasPermission(grouppermission));
	EXPECT_EQ(true, user2->hasPermission(grouppermission));



	// create artifact
	auto artifact = UserDB::createArtifact(*user, "project", "Test Project", "test_project");
	EXPECT_EQ("project", artifact->getType());
	EXPECT_EQ("Test Project", artifact->getName());
	EXPECT_EQ("test_project", artifact->getLatestArtifactVersion()->getValue());

	// update
	now++; // increase time one second, because user + time has to be unique for an artifact version
	artifact->updateValue("new value");

	// load artifact
	artifact = UserDB::loadArtifact(*user, user->getUsername(), "project", "Test Project");
	EXPECT_EQ(2, artifact->getVersions().size());

	auto version0 = artifact->getArtifactVersion(artifact->getVersions().at(0));
	auto version1 = artifact->getArtifactVersion(artifact->getVersions().at(1));
	EXPECT_EQ(true, version0->getTimestamp() > version1->getTimestamp());
	EXPECT_EQ("new value", version0->getValue());
	EXPECT_EQ("test_project", version1->getValue());

	// load artifacts of type
	now++; // increase time by 1 second
	artifact = UserDB::createArtifact(*user, "project", "Test Project 2", "test_project 2");
	artifact = UserDB::createArtifact(*user, "rscript", "My R script", "1 + 2");

	auto artifacts = UserDB::loadArtifactsOfType(*user, "project");
	EXPECT_EQ(2, artifacts.size());
	EXPECT_EQ("Test Project 2", artifacts[0].getName());
	EXPECT_EQ("Test Project", artifacts[1].getName());

	// share
	// user2 has no access
	user2 = UserDB::createUser(username + "2", "realname", "email", password);
	EXPECT_THROW(UserDB::loadArtifact(*user2, user->getUsername(), "project", "Test Project"), UserDB::authorization_error);
	artifact = UserDB::loadArtifact(*user, user->getUsername(), "project", "Test Project");
	user2 = artifact->shareWithUser(user2->getUsername());

	// user2 now has access
	EXPECT_NO_THROW(UserDB::loadArtifact(*user2, user->getUsername(), "project", "Test Project"));

	artifact = UserDB::loadArtifact(*user, user->getUsername(), "rscript", "My R script");
	user2 = artifact->shareWithUser(user2->getUsername());

	// check that shared artifacts of type are listed
	artifacts = UserDB::loadArtifactsOfType(*user2, "project");
	EXPECT_EQ(1, artifacts.size());
	EXPECT_EQ("Test Project", artifacts[0].getName());
}
