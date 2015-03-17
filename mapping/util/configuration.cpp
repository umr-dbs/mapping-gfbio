
#include "util/configuration.h"
#include <map>
#include <iostream>
#include <fstream>
#include <sstream>

static std::map<std::string, std::string> values;


static std::string stripWhitespace(const std::string &str) {
	const char *whitespace = " \t\r\n";
	auto start = str.find_first_not_of(whitespace);
	auto end = str.find_last_not_of(whitespace);
	if (start == std::string::npos || end == std::string::npos)
		return std::string("");

	return str.substr(start, end-start+1);
}


void Configuration::load(const std::string &filename) {
	std::ifstream file(filename);
	if (!file.good())
		return;
	std::string line;
	int linenumber = 0;
	while (std::getline(file, line)) {
		linenumber++;
		line = stripWhitespace(line);
		if (line.length() == 0 || line[0] == '#')
			continue;

		auto pos = line.find_first_of('=');
		if (pos == std::string::npos) {
			std::cerr << "Error in configuration file on line " << linenumber << ": not a key=value pair\n" << line << "\n";
			continue;
		}

		std::string key = stripWhitespace(line.substr(0, pos));
		std::string value = stripWhitespace(line.substr(pos+1));
		if (key == "")
			continue;

		values[key] = value;
	}
}

void Configuration::loadFromDefaultPaths() {
	load("/etc/mapping.conf");
	load("./mapping.conf");
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
