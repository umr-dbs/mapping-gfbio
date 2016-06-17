
#ifndef UTIL_CONFIGURATION_H_
#define UTIL_CONFIGURATION_H_

#include <string>

/**
 * Class for loading the configuration of the application.
 * It loads key, value parameters from the following location in the given order
 * 1. /etc/mapping.conf
 * 2. working directory mapping.conf
 * 3. environment variables starting with MAPPING_ and mapping_
 *
 */
class Configuration {
	public:
		static void loadFromDefaultPaths();
		static void load(const std::string &filename);
	private:
		static void loadFromEnvironment();
		static void parseLine(const std::string &line);
	public:
		static const std::string &get(const std::string &name);
		static const std::string &get(const std::string &name, const std::string &defaultValue);
		static int getInt(const std::string &name);
		static int getInt(const std::string &name, const int defaultValue);
		static bool getBool(const std::string &name);
		static bool getBool(const std::string &name, const bool defaultValue);

		// These parsers are helper methods.
		// They're only exported publicly because HTTPService::Params wants to use them.
		// They do throw exceptions when the string cannot be parsed.
		static int parseInt(const std::string &str);
		static bool parseBool(const std::string &str);

};

#endif
