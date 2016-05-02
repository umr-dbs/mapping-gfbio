
#include "util/exceptions.h"
#include "util/sqlite.h"

#include <sqlite3.h>

#include <sstream>
#include <string>


SQLite::SQLite() : db(nullptr) {

}

SQLite::~SQLite() {
	if (db) {
		sqlite3_close(db);
		db = nullptr;
	}
}

void SQLite::open(const char *filename, bool readonly) {
	if (db)
		throw SQLiteException("DB already open");

	int rc = sqlite3_open_v2(filename, &db, readonly ? (SQLITE_OPEN_READONLY) : (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE), nullptr);
	if (rc != SQLITE_OK)
		throw SQLiteException(concat("Can't open database ", filename, ": ", sqlite3_errmsg(db)));
}


void SQLite::exec(const char *query) {
	char *error = 0;
	if (SQLITE_OK != sqlite3_exec(db, query, NULL, NULL, &error)) {
		auto msg = concat("Error on query ", query, ": ", error);
		sqlite3_free(error);
		throw SQLiteException(msg);
	}
}

SQLite::SQLiteStatement SQLite::prepare(const char *query) {
	return SQLiteStatement(db, query);
}

int64_t SQLite::getLastInsertId() {
	return sqlite3_last_insert_rowid(db);
}




SQLite::SQLiteStatement::SQLiteStatement(sqlite3 *db, const char *query) : stmt(nullptr) {
	auto result = sqlite3_prepare_v2(
		db,
		query,
		-1, // read until '\0'
		&stmt,
		NULL
	);
	if (result != SQLITE_OK)
		throw SQLiteException(concat("Cannot prepare statement: ", result, ", error='", sqlite3_errmsg(db), "', query='", query, "'"));
}

SQLite::SQLiteStatement::~SQLiteStatement() {
	finalize();
}

SQLite::SQLiteStatement::SQLiteStatement(SQLiteStatement &&other) : stmt(nullptr) {
	std::swap(stmt, other.stmt);
}

SQLite::SQLiteStatement &SQLite::SQLiteStatement::operator=(SQLiteStatement &&other) {
	std::swap(stmt, other.stmt);
	return *this;
}

void SQLite::SQLiteStatement::bind(int idx, int32_t value) {
	if (!stmt)
		throw SQLiteException("Prepare before binding");

	if (SQLITE_OK != sqlite3_bind_int(stmt, idx, value) )
		throw SQLiteException("error binding int");
}
void SQLite::SQLiteStatement::bind(int idx, int64_t value) {
	if (!stmt)
		throw SQLiteException("Prepare before binding");

	if (SQLITE_OK != sqlite3_bind_int64(stmt, idx, value) )
		throw SQLiteException("error binding int");
}
void SQLite::SQLiteStatement::bind(int idx, double value) {
	if (!stmt)
		throw SQLiteException("Prepare before binding");

	if (SQLITE_OK != sqlite3_bind_double(stmt, idx, value) )
		throw SQLiteException("error binding double");
}
void SQLite::SQLiteStatement::bind(int idx, const char *value) {
	if (!stmt)
		throw SQLiteException("Prepare before binding");

	if (SQLITE_OK != sqlite3_bind_text(stmt, idx, value, -1, SQLITE_TRANSIENT) )
		throw SQLiteException("error binding text");
}


void SQLite::SQLiteStatement::exec() {
	if (!stmt)
		throw SQLiteException("Prepare before exec");

	if (SQLITE_DONE != sqlite3_step(stmt))
		throw SQLiteException("SQLiteStatement::exec() failed");
	if (SQLITE_OK != sqlite3_reset(stmt))
		throw SQLiteException("SQLiteStatement::reset failed");
}

bool SQLite::SQLiteStatement::next() {
	if (!stmt)
		throw SQLiteException("Prepare before next");

	int res = sqlite3_step(stmt);

	if (res == SQLITE_ROW)
		return true;
	if (res == SQLITE_DONE) {
		if (SQLITE_OK != sqlite3_reset(stmt))
			throw SQLiteException("SQLiteStatement::reset failed");
		return false;
	}
	throw SQLiteException("SQLiteStatement::next failed");
}


int32_t SQLite::SQLiteStatement::getInt(int column) {
	return sqlite3_column_int(stmt, column);
}

int64_t SQLite::SQLiteStatement::getInt64(int column) {
	return sqlite3_column_int64(stmt, column);
}

double SQLite::SQLiteStatement::getDouble(int column) {
	return sqlite3_column_double(stmt, column);
}

const char *SQLite::SQLiteStatement::getString(int column) {
	return (const char *) sqlite3_column_text(stmt, column);
}


void SQLite::SQLiteStatement::finalize() {
	if (stmt) {
		sqlite3_finalize(stmt);
		stmt = nullptr;
	}
}
