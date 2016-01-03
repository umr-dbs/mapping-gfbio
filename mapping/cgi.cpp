#include "util/configuration.h"
#include "util/debug.h"
#include "cache/manager.h"
#include "util/timeparser.h"

#include "services/httpservice.h"


#include <cstdio>
#include <cstdlib>
#include <string>

#include <json/json.h>

#include <fcgio.h>


/*
A few benchmarks:
SAVE_PNG8:   0.052097
SAVE_PNG32:  0.249503
SAVE_JPEG8:  0.021444 (90%)
SAVE_JPEG32: 0.060772 (90%)
SAVE_JPEG8:  0.021920 (100%)
SAVE_JPEG32: 0.060187 (100%)

Sizes:
JPEG8:  200526 (100%)
PNG8:   159504
JPEG8:  124698 (95%)
JPEG8:   92284 (90%)

PNG32:  366925
JPEG32: 308065 (100%)
JPEG32: 168333 (95%)
JPEG32: 120703 (90%)
*/



int main() {
	Configuration::loadFromDefaultPaths();
	bool cache_enabled = Configuration::getBool("cache.enabled",false);

	std::unique_ptr<CacheManager> cm;

	// Plug in Cache-Dummy if cache is disabled
	if ( !cache_enabled ) {
		cm = make_unique<NopCacheManager>();
	}
	else {
		std::string host = Configuration::get("indexserver.host");
		int port = atoi( Configuration::get("indexserver.port").c_str() );
		cm = make_unique<ClientCacheManager>(host,port);
	}
	CacheManager::init( cm.get() );


	if (getenv("FCGI_WEB_SERVER_ADDRS") == nullptr) {
		// CGI mode
		HTTPService::run(std::cin.rdbuf(), std::cout.rdbuf(), std::cerr.rdbuf());
	}
	else {
		// FCGI mode
		FCGX_Request request;
		int res;
		res = FCGX_Init();
		if (res != 0)
			throw std::runtime_error("FCGX_Init failed");
		res = FCGX_InitRequest(&request, 0, 0);
		if (res != 0)
			throw std::runtime_error("FCGX_InitRequest failed");

		while (FCGX_Accept_r(&request) == 0) {
			fcgi_streambuf streambuf_in(request.in);
			fcgi_streambuf streambuf_out(request.out);
			fcgi_streambuf streambuf_err(request.err);

			HTTPService::run(&streambuf_in, &streambuf_out, &streambuf_err);
		}
	}
}
