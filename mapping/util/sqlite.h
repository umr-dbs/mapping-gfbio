#ifndef UTIL_SQLITE_H
#define UTIL_SQLITE_H


class sqlite3;
class sqlite3_stmt;
#include <string>
#include <stdint.h>

class SQLite {
	private:
		class SQLiteStatement {
			public:
				SQLiteStatement(sqlite3 *db, const char *query);
				~SQLiteStatement();
				SQLiteStatement(const SQLiteStatement &other) = delete;
				// this class is supposed to be movable, so SQLite::prepare can work
				SQLiteStatement(SQLiteStatement &&other);
				SQLiteStatement &operator=(SQLiteStatement &&other);

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
				double getDouble(int column);
				const char *getString(int column);

				// cleanup for re-use
				void finalize();
			private:
				sqlite3_stmt *stmt;
		};
	public:
		SQLite();
		~SQLite();
		void open(const char *filename, bool readonly = false);
		SQLiteStatement prepare(const char *query);
		SQLiteStatement prepare(const std::string &query) { return prepare(query.c_str()); }
		void exec(const char *query);
		int64_t getLastInsertId();
	private:
		sqlite3 *db;

};





#endif
