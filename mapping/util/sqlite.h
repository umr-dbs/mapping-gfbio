#ifndef UTIL_SQLITE_H
#define UTIL_SQLITE_H


class sqlite3;
class sqlite3_stmt;
#include <string>
#include <stdint.h>


class SQLite {
	public:
		SQLite();
		~SQLite();
		void open(const char *filename);
		void exec(const char *query);
	private:
		sqlite3 *db;

	friend class SQLiteStatement;
};


class SQLiteStatement {
	public:
		SQLiteStatement(const SQLite &sqlite);
		~SQLiteStatement();

		// prepare a statement
		void prepare(const char *query);
		void prepare(const std::string &query) { prepare(query.c_str()); }

		// bind parameters
		void bind(int idx, int32_t value);
		void bind(int idx, int64_t value);
		void bind(int idx, double value);
		void bind(int idx, const char *value);
		void bind(int idx, const std::string &value) { bind(idx, value.c_str()); }

		// execute
		void exec();

		// select
		bool next();
		int32_t getInt(int column);
		int64_t getInt64(int column);

		// cleanup for re-use
		void finalize();
	private:
		const SQLite *db;
		sqlite3_stmt *stmt;
};


#endif
