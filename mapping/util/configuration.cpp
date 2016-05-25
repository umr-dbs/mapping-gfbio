
#include "util/exceptions.h"
#include "util/configuration.h"
#include <map>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <algorithm>
#include <cstdlib>
#include <vector>

#include <unistd.h>
#include <sys/types.h>
#include <pwd.h>

#include <unistd.h>
#include <string.h>
extern char **environ;

static std::map<std::string, std::string> values;


static std::string stripWhitespace(const std::string &str) {
	const char *whitespace = " \t\r\n";
	auto start = str.find_first_not_of(whitespace);
	auto end = str.find_last_not_of(whitespace);
	if (start == std::string::npos || end == std::string::npos)
		return std::string("");

	return str.substr(start, end-start+1);
}

static std::string normalizeKey(const std::string &_key) {
	auto key = stripWhitespace(_key);
	for (size_t i=0;i<key.length();i++) {
		char c = key[i];
		if (c >= 'a' && c <= 'z')
			continue;
		if (c >= 'A' && c <= 'Z') {
			key[i] = c - 'A' + 'a';
			continue;
		}
		if (c == '_') {
			key[i] = '.';
			continue;
		}
		if ((c >= '0' && c <= '9') || (c == '.'))
			continue;

		// some other character -> the whole key is invalid
		std::cerr << "Error in configuration: invalid key name '" << _key << "'\n";
		return "";
	}
	return key;
}


void Configuration::parseLine(const std::string &_line) {
	auto line = stripWhitespace(_line);
	if (line.length() == 0 || line[0] == '#')
		return;

	auto pos = line.find_first_of('=');
	if (pos == std::string::npos) {
		std::cerr << "Error in configuration: not a key=value pair, line = '" << line << "'\n";
		return;
	}

	std::string key = normalizeKey(line.substr(0, pos));
	if (key == "")
		return;
	std::string value = stripWhitespace(line.substr(pos+1));

	values[key] = value;
}


void Configuration::load(const std::string &filename) {
	std::ifstream file(filename);
	if (!file.good())
		return;
	std::string line;
	while (std::getline(file, line))
		parseLine(line);
}


void Configuration::loadFromEnvironment() {
	if (environ == nullptr)
		return;

	std::string configuration_file;
	std::vector<std::string> relevant_vars;

	for(int i=0;environ[i] != nullptr;i++) {
		auto line = environ[i];
		if (strncmp(line, "MAPPING_", 8) == 0 || strncmp(line, "mapping_", 8) == 0) {
			std::string linestr(&line[8]);
			if (strncmp(linestr.c_str(), "CONFIGURATION=", 14) || strncmp(linestr.c_str(), "configuration=", 14))
				configuration_file = linestr.substr(14);
			else
				relevant_vars.push_back(linestr);
		}
	}

	// The file must be loaded before we parse the variables, to guarantee a repeatable priority when multiple settings overlap
	if (configuration_file != "")
		load(configuration_file);
	for (auto &linestr : relevant_vars)
		parseLine(linestr);
}

static const char* getHomeDirectory() {
	// Note that $HOME is not set for the cgi-bin executed by apache
	auto homedir = getenv("HOME");
	if (homedir)
		return homedir;

	auto pw = getpwuid(getuid());
	return pw->pw_dir;

	return nullptr;
}

static bool loaded_from_default_paths = false;
void Configuration::loadFromDefaultPaths() {
	if (loaded_from_default_paths)
		return;
	loaded_from_default_paths = true;

	load("/etc/mapping.conf");

	auto homedir = getHomeDirectory();
	if (homedir && strlen(homedir) > 0) {
		std::string path = "";
		path += homedir;
		path += "/mapping.conf";
		load(path.c_str());
	}

	load("./mapping.conf");
	loadFromEnvironment();
}


const std::string &Configuration::get(const std::string &name) {
	auto it = values.find(name);
	if (it == values.end())
		throw ArgumentException(concat("No configuration found for key ", name));
	return it->second;
}


const std::string &Configuration::get(const std::string &name, const std::string &defaultValue) {
	auto it = values.find(name);
	if (it == values.end())
		return defaultValue;
	return it->second;
}

int Configuration::getInt(const std::string &name) {
	auto it = values.find(name);
	if (it == values.end())
		throw ArgumentException(concat("No configuration found for key ", name));
	return parseInt(it->second);
}

int Configuration::getInt(const std::string &name, const int defaultValue) {
	auto it = values.find(name);
	if (it == values.end())
		return defaultValue;
	return parseInt(it->second);
}

bool Configuration::getBool(const std::string &name) {
	auto it = values.find(name);
	if (it == values.end())
		throw ArgumentException(concat("No configuration found for key ", name));
	return parseBool(it->second);
}

bool Configuration::getBool(const std::string &name, const bool defaultValue) {
	auto it = values.find(name);
	if (it == values.end())
		return defaultValue;
	return parseBool(it->second);
}


int Configuration::parseInt(const std::string &str) {
	return std::stoi(str); // stoi throws if no conversion could be performed
}

bool Configuration::parseBool(const std::string &str) {
	if (str == "1")
		return true;
	if (str == "0")
		return false;
	std::string strtl;
	strtl.resize( str.length() );
	std::transform(str.cbegin(), str.cend(), strtl.begin(), ::tolower);

	if (strtl == "true" || strtl == "yes")
		return true;
	if (strtl == "false" || strtl == "no")
		return false;

	throw ArgumentException(concat("'", str, "' is not a boolean value (try setting 0/1, yes/no or true/false)"));
}
