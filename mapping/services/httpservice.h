#ifndef SERVICES_HTTPSERVICES_H
#define SERVICES_HTTPSERVICES_H

#include "util/make_unique.h"

#include <memory>
#include <string>
#include <map>
#include <iostream>


class HTTPService {
	public:
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

		HTTPService(const Params& params, HTTPResponseStream& result, std::ostream &error);
	protected:
		HTTPService(const HTTPService &other) = delete;
		HTTPService &operator=(const HTTPService &other) = delete;

		virtual void run() = 0;
		static std::unique_ptr<HTTPService> getRegisteredService(const std::string &name,const Params& params, HTTPResponseStream& result, std::ostream &error);

		const Params &params;
		HTTPResponseStream &result;
		std::ostream &error;
	public:
		virtual ~HTTPService() = default;

		static void run(std::streambuf *in, std::streambuf *out, std::streambuf *err);
};



class HTTPServiceRegistration {
	public:
		HTTPServiceRegistration(const char *name, std::unique_ptr<HTTPService> (*constructor)(const HTTPService::Params &params, HTTPService::HTTPResponseStream &result, std::ostream &error));
};

#define REGISTER_HTTP_SERVICE(classname, name) static std::unique_ptr<HTTPService> create##classname(const HTTPService::Params &params, HTTPService::HTTPResponseStream &result, std::ostream &error) { return make_unique<classname>(params, result, error); } static HTTPServiceRegistration register_##classname(name, create##classname)


#endif
