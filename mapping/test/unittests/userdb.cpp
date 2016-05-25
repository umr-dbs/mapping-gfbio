#include "userdb/userdb.h"
#include "util/configuration.h"
#include "util/exceptions.h"

#include <gtest/gtest.h>



TEST(UserDB, testALL) {
	// We must protect against overwriting the production DB, so make sure to use a custom configuration here!
	// init() will throw if the UserDB was initialized before.
	UserDB::init("sqlite", ":memory:");

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


}
