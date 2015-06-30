
#include "util/exceptions.h"
#include "util/curl.h"

#include <curl/curl.h>
#include <mutex>
#include <memory>
#include <sstream>


static std::mutex curl_init_mutex;
static bool curl_is_initialized = false;

void curl_init() {
	if (curl_is_initialized)
		return;
	std::lock_guard<std::mutex> guard(curl_init_mutex);
	if (curl_is_initialized)
		return;
	curl_is_initialized = true;
	CURLcode success = curl_global_init(CURL_GLOBAL_DEFAULT);
	if (success != CURLE_OK) {
		fprintf(stderr, "Failed to initialize curl, aborting\n");
		exit(5);
	}
}




cURL::cURL() : handle(nullptr) {
	curl_init();
	handle = curl_easy_init();
	setOpt(CURLOPT_ERRORBUFFER, errorbuffer);
}

cURL::~cURL() {
	if (handle) {
		curl_easy_cleanup(handle);
		handle = nullptr;
	}
}


void cURL::perform() {
	CURLcode success = curl_easy_perform(handle);
	if (success != CURLE_OK)
		throw cURLException(concat("cURL::perform(): ", success, " ", errorbuffer));
}



size_t cURL::defaultWriteFunction(void *buffer, size_t size, size_t nmemb, void *userp) {
	std::stringstream *ss = (std::stringstream *) userp;

	size_t totalsize = size * nmemb;

	ss->write((const char *) buffer, totalsize);

	return totalsize;
}


std::string cURL::escape(const std::string &input) {
	char *escaped = curl_easy_escape(handle, input.c_str(), input.length());

	std::string result(escaped);
	curl_free(escaped);
	return result;
}
