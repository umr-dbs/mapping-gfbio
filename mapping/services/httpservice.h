#ifndef SERVICES_HTTPSERVICES_H
#define SERVICES_HTTPSERVICES_H

#include "util/make_unique.h"

#include <memory>
#include <string>
#include <map>
#include <ostream>


class HTTPService {
	protected:
		// class Params
		class Params : public std::map<std::string, std::string> {
			public:
				bool hasParam(const std::string& key) const {
					return find(key) != end();
				}

				std::string get(const std::string &name) const;
				std::string get(const std::string &name, const std::string &defaultValue) const;
				int getInt(const std::string &name) const;
				int getInt(const std::string &name, int defaultValue) const;
				bool getBool(const std::string &name) const;
				bool getBool(const std::string &name, bool defaultValue) const;
		};

		// class ResponseStream
		class HTTPResponseStream : public std::ostream {
			public:
				HTTPResponseStream(std::streambuf *buf);
				virtual ~HTTPResponseStream();
				void send500(const std::string &message);
				void sendHeader(const std::string &key, const std::string &value);
				void sendContentType(const std::string &contenttype);
				void sendDebugHeader();
				void finishHeaders();

				bool hasSentHeaders();
			private:
				bool headers_sent;
		};


		HTTPService() = default;
		HTTPService(const HTTPService &other) = delete;
		HTTPService &operator=(const HTTPService &other) = delete;

		virtual void run(const Params& params, HTTPResponseStream& result, std::ostream &error) = 0;
		static std::unique_ptr<HTTPService> getRegisteredService(const std::string &name);

		static Params parseQueryString(const char *query_string);

	public:
		virtual ~HTTPService() = default;

		static void run(const char *query_string, std::streambuf *out, std::streambuf *err);
};



class HTTPServiceRegistration {
	public:
		HTTPServiceRegistration(const char *name, std::unique_ptr<HTTPService> (*constructor)());
};

#define REGISTER_HTTP_SERVICE(classname, name) static std::unique_ptr<HTTPService> create##classname() { return make_unique<classname>(); } static HTTPServiceRegistration register_##classname(name, create##classname)


#endif
