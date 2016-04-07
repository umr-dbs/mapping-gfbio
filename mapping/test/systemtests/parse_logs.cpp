

/*
 * This program takes a bunch of test-logs and outputs the results in JUnit's XML format.
 *
 * A documentation of the XML format can be found here:
 * https://github.com/windyroad/JUnit-Schema/blob/master/JUnit.xsd
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>

#include <string>
#include <vector>
#include <algorithm> // sort(), max()

#include <ctime>

#include <sys/types.h>
#include <unistd.h>
#include <dirent.h> // opendir(), ..


static const char *LOG_ROOT = "test/systemtests/queries/";


// mapping's core system does not require an XML library, and we do not
// want to introduce another dependency just for a testcase helper binary.
// Instead, we're using the good old XML-via-concatenation approach.
static void encodeXML(std::string &out, const std::string &in) {
	for(size_t pos = 0; pos != in.size(); pos++) {
		switch(in[pos]) {
				case '&':  out.append("&amp;");       break;
				case '\"': out.append("&quot;");      break;
				case '\'': out.append("&apos;");      break;
				case '<':  out.append("&lt;");        break;
				case '>':  out.append("&gt;");        break;
				default:   out.append(&in[pos], 1); break;
		}
	}
}

// We are aware of c++11's std::regex, but it's not fully supported in gcc 4.8, which we're using.
// again, we do not want to bump our compiler requirements for a a testcase helper binary.
static bool str_starts_with(const std::string &subject, const std::string &prefix, std::string &suffix) {
	if (subject.substr(0, prefix.length()) == prefix) {
		suffix = subject.substr(prefix.length());
		return true;
	}
	suffix = "";
	return false;
}

static std::string getIsoDate() {
	auto t = std::time(nullptr);
	if (t < 0)
		return "";
	std::tm *tm = std::gmtime(&t);
	if (tm == nullptr)
		return "";

	int year = 1900 + tm->tm_year;
	int month = tm->tm_mon + 1;
	int day = tm->tm_mday;
	int hour = tm->tm_hour;
	int minute = tm->tm_min;
	int second = tm->tm_sec;

	std::ostringstream result;
	result.fill('0');

	result << std::setw(4) << year << "-" << std::setw(2) << month << "-" <<std::setw(2) << day << "T"
	       << std::setw(2) << hour << ":" <<std::setw(2) << minute << ":" <<std::setw(2) << second << "Z";

	return result.str();
}


int main() {
	// Find all log files
	DIR *dir = opendir(LOG_ROOT);
	if (!dir) {
		perror("opendir failed");
		exit(5);
	}

	std::vector<std::string> lognames;
	while (auto dirent = readdir(dir)) {
		std::string name(dirent->d_name);
		if (name.length() < 4 || name.substr(name.length() - 4, 4) != ".log")
			continue;

		lognames.push_back(name.substr(0, name.length()-4));
	}
	closedir(dir);

	std::sort(lognames.begin(), lognames.end());

	int tests_total = 0;
	int tests_errors = 0;
	int tests_failures = 0;
	double tests_totaltime = 0;
	std::string xml;

	for (auto &logname : lognames) {
		// Load and parse the logfile
		std::ifstream stream(std::string(LOG_ROOT) + logname + ".log", std::ios::in | std::ios::binary);
		if (!stream)
			throw std::runtime_error("Could not open logfile");

		std::string full_log;
		bool passed_semantic = false;
		bool passed_hash = false;
		bool passed_sanitizer = true;
		double elapsedtime = -1;
		int returncode = -1;

		// iterate over all lines in the log
		std::string line;
		while (std::getline(stream,  line)) {
			std::string suffix;
			if (str_starts_with(line, "TESTCASE_ELAPSED_TIME: ", suffix)) {
				elapsedtime = std::stod(suffix);
			}
			if (str_starts_with(line, "TESTCASE_RETURN_CODE: ", suffix)) {
				returncode = std::stoi(suffix);
			}
			if (str_starts_with(line, "PASSED: semantic", suffix)) {
				passed_semantic = true;
			}
			if (str_starts_with(line, "PASSED: hash", suffix)) {
				passed_hash = true;
			}
			if (line.find("AddressSanitizer") != std::string::npos || line.find("LeakSanitizer") != std::string::npos || line.find(": runtime error: ") != std::string::npos) {
				passed_sanitizer = false;
			}

			full_log += line + "\n";
		}
		stream.close();

		// find total test status
		// These are test failures, e.g. the test had the wrong result
		bool has_failure = false;
		if (!passed_semantic || !passed_hash)
			has_failure = true;

		// These are test errors, e.g. the test crashed or did not run correctly
		bool has_error = false;
		if (returncode != 0 || !(elapsedtime > 0) || !passed_sanitizer)
			has_error = true;

		// accumulate global stats
		tests_total++;
		if (has_error)
			tests_errors++;
		else if (has_failure)
			tests_failures++;
		tests_totaltime += std::max(0.0, elapsedtime);

		// append to XML output
		xml += "<testcase name=\"";
		encodeXML(xml, logname);
		xml += "\" classname=\"systemtests.";
		encodeXML(xml, logname);
		xml += "\" status=\"run\"";
		if (elapsedtime > 0)
			xml += " time=\"" + std::to_string(elapsedtime) + "\"";
		xml += ">";
		if (has_failure || has_error) {
				xml += (has_error ? "<error" : "<failure");
				xml += " message=\"Failed:";
				if (returncode != 0)
					xml += " returncode";
				if (elapsedtime <= 0)
					xml += " elapsedtime";
				if (!passed_sanitizer)
					xml += " sanitizer";
				if (!passed_semantic)
					xml += " semantic";
				if (!passed_hash)
					xml += " hash";
				xml += "\">";
				encodeXML(xml, full_log);
				xml += (has_error ? "</error>" : "</failure>");
		}

		xml += "</testcase>\n";
	}

	// Finalize XML output
	std::string xml_header;
	xml_header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
	xml_header += "<testsuites timestamp=\"";
	encodeXML(xml_header, getIsoDate());
	xml_header += "\" name=\"AllSystemtests\" tests=\"" + std::to_string(tests_total) + "\" failures=\"" + std::to_string(tests_failures) + "\" disabled=\"0\" errors=\"" + std::to_string(tests_errors) + "\" time=\"" + std::to_string(tests_totaltime) + "\">\n";
	xml_header += "<testsuite name=\"Systemtests\" tests=\"" + std::to_string(tests_total) + "\" failures=\"" + std::to_string(tests_failures) + "\" disabled=\"0\" errors=\"" + std::to_string(tests_errors) + "\" time=\"" + std::to_string(tests_totaltime) + "\">\n";

	std::string xml_footer;
	xml_footer = "</testsuite></testsuites>\n";

	std::cout << xml_header << xml << xml_footer;
}

