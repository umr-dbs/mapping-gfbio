
#ifndef UTIL_CONFIGURATION_H_
#define UTIL_CONFIGURATION_H_

#include <string>
#include <map>

class Parameters : public std::map<std::string, std::string> {
	public:
		bool hasParam(const std::string& key) const;

		const std::string &get(const std::string &name) const;
		const std::string &get(const std::string &name, const std::string &defaultValue) const;
		int getInt(const std::string &name) const;
		int getInt(const std::string &name, int defaultValue) const;
		bool getBool(const std::string &name) const;
		bool getBool(const std::string &name, bool defaultValue) const;

		/**
		 * Returns all parameters with a given prefix, with the prefix stripped.
		 * For example, if you have the configurations
		 *  my.module.paramA = 50
		 *  my.module.paramB = 20
		 * then parameters.getPrefixedParameters("my.module.") will return a Parameters object with
		 *  paramA = 50
		 *  paramB = 20
		 *
		 * @param prefix the prefix of the interesting parameter names. Usually, this should end with a dot.
		 */
		Parameters getPrefixedParameters(const std::string &prefix);

		// These do throw exceptions when the string cannot be parsed.
		static int parseInt(const std::string &str);
		static bool parseBool(const std::string &str);
};

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
		static const std::string &get(const std::string &name) {
			return parameters.get(name);
		}
		static const std::string &get(const std::string &name, const std::string &defaultValue) {
			return parameters.get(name, defaultValue);
		}
		static int getInt(const std::string &name) {
			return parameters.getInt(name);
		}
		static int getInt(const std::string &name, const int defaultValue) {
			return parameters.getInt(name, defaultValue);
		}
		static bool getBool(const std::string &name) {
			return parameters.getBool(name);
		}
		static bool getBool(const std::string &name, const bool defaultValue) {
			return parameters.getBool(name, defaultValue);
		}
		static Parameters getPrefixedParameters(const std::string &prefix) {
			return parameters.getPrefixedParameters(prefix);
		}
	private:
		static Parameters parameters;
};

#endif
