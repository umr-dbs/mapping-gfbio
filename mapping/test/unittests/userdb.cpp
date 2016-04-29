#include "userdb/userdb.h"
#include "util/configuration.h"
#include "util/exceptions.h"

#include <gtest/gtest.h>

TEST(UserDB, testALL) {
	// We must protect against overwriting the production DB, so make sure to use a custom configuration here!
	// init() will throw if the UserDB was initialized before.
	UserDB::init("sqlite", ":memory:");

	const std::string username = "dummy";
	const std::string password = "opensesame";

	// Create a user
	auto user = UserDB::createUser(username, password);
	EXPECT_EQ(user->getUsername(), username);
	EXPECT_EQ(user->hasPermission(concat("user.", user->getUserID())), true);

	// create sessions
	EXPECT_THROW(UserDB::createSession(username, "wrong password"), UserDB::credentials_error);

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
}
