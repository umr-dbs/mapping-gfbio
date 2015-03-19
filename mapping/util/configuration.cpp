
#include "util/configuration.h"
#include <map>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <algorithm>

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
	for(int i=0;environ[i] != nullptr;i++) {
		auto line = environ[i];
		if (strncmp(line, "MAPPING_", 8) == 0 || strncmp(line, "mapping_", 8) == 0) {
			std::string linestr(&line[8]);
			parseLine(linestr);
		}
	}
}


void Configuration::loadFromDefaultPaths() {
	load("/etc/mapping.conf");
	load("./mapping.conf");
	loadFromEnvironment();
	//for (auto key : values)
	//	std::cerr << key.first << " = " << key.second << "\n";
}


const std::string &Configuration::get(const std::string &name) {
	return values.at(name);
}


const std::string &Configuration::get(const std::string &name, const std::string &defaultValue) {
	auto it = values.find(name);
	if (it == values.end())
		return defaultValue;
	return it->second;
}
