
#ifndef UTIL_CONFIGURATION_H_
#define UTIL_CONFIGURATION_H_

#include <string>

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
};

#endif
