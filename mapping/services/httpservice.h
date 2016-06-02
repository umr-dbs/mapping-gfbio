#ifndef SERVICES_HTTPSERVICES_H
#define SERVICES_HTTPSERVICES_H

#include "util/make_unique.h"

#include <memory>
#include <string>
#include <map>
#include <iostream>

#include <json/json.h>

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
				/**
				 * Sends a 500 Internal Server Error with the given message
				 */
				void send500(const std::string &message);
				/**
				 * Sends a HTTP header
				 */
				void sendHeader(const std::string &key, const std::string &value);
				/**
				 * Shorthand for sending a HTTP header indicating the content-type
				 */
				void sendContentType(const std::string &contenttype);
				void sendDebugHeader();
				/**
				 * Indicates that all headers have been sent, readying the stream for
				 * sending the content via operator<<
				 */
				void finishHeaders();

				bool hasSentHeaders();

				/**
				 * Sends appropriate headers followed by the serialized JSON object
				 */
				void sendJSON(const Json::Value &obj);
				/**
				 * Sends a json object with an additional value "result": true
				 *
				 * These are used for internal protocols. The result is guaranteed to be a JSON object
				 * with a "result" attribute, which is either true or a string containing an error message.
				 */
				void sendSuccessJSON(Json::Value &obj);
				void sendSuccessJSON();
				template<typename T>
				void sendSuccessJSON(const std::string &key, const T& value) {
					Json::Value obj(Json::ValueType::objectValue);
					obj[key] = value;
					sendSuccessJSON(obj);
				}
				/**
				 * Sends a json object indicating failure.
				 */
				void sendFailureJSON(const std::string &error);

			private:
				bool headers_sent;
		};

		HTTPService(const Params& params, HTTPResponseStream& response, std::ostream &error);
	protected:
		HTTPService(const HTTPService &other) = delete;
		HTTPService &operator=(const HTTPService &other) = delete;

		virtual void run() = 0;
		static std::unique_ptr<HTTPService> getRegisteredService(const std::string &name,const Params& params, HTTPResponseStream& response, std::ostream &error);

		const Params &params;
		HTTPResponseStream &response;
		std::ostream &error;
	public:
		virtual ~HTTPService() = default;

		static void run(std::streambuf *in, std::streambuf *out, std::streambuf *err);
};



class HTTPServiceRegistration {
	public:
		HTTPServiceRegistration(const char *name, std::unique_ptr<HTTPService> (*constructor)(const HTTPService::Params &params, HTTPService::HTTPResponseStream &response, std::ostream &error));
};

#define REGISTER_HTTP_SERVICE(classname, name) static std::unique_ptr<HTTPService> create##classname(const HTTPService::Params &params, HTTPService::HTTPResponseStream &response, std::ostream &error) { return make_unique<classname>(params, response, error); } static HTTPServiceRegistration register_##classname(name, create##classname)


#endif
