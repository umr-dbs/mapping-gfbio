/*
 * server.cpp
 *
 *  Created on: 18.05.2015
 *      Author: mika
 */

#include <gtest/gtest.h>
#include "test/unittests/cache/util.h"
#include "util/configuration.h"
#include "cache/server.h"

#include <iostream>


using namespace std;

TEST(CacheServerTest,SimpleTest) {
	Configuration::loadFromDefaultPaths();

	std::unique_ptr<CacheManager> mgrImpl( new DefaultCacheManager(5 * 1024 * 1024) );
	//std::unique_ptr<CacheManager> mgrImpl( new NopCacheManager() );

	CacheManager::init( mgrImpl );

	const char *json = "{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}";
	std::string timestr("2010-06-06T18:00:00.000Z");
	epsg_t epsg = EPSG_LATLON;
	uint32_t width = 256, height = 256;
	time_t timestamp = parseIso8601DateTime(timestr);

	std::string bboxes[] = {
		std::string("45,-180,67.5,-157.5"),
		std::string("45,-157.5,67.5,-135"),
		std::string("45,-135,67.5,-112.5"),
		std::string("45,-112.5,67.5,-90")
	};
	double bbox[4];

	CacheServer cs(12346,4);
	unique_ptr<thread> cst = cs.runAsync();

	for ( int i = 0; i < 4; i++ ) {
		parseBBOX(bbox, bboxes[i], epsg, false);
		QueryRectangle qr(timestamp, bbox[0], bbox[1], bbox[2], bbox[3], width, height, epsg);

		RasterRequest rr(json,qr,GenericOperator::RasterQM::EXACT);

		// Connect to server
		UnixSocket socket("localhost", 12346);
		rr.toStream( socket );

		RasterResponse res(socket);

		if ( !res.success )
			cout << res.message << endl;
		else {
			string strefstr = strefToString( res.data->stref );
			cout << "Result: " << strefstr << endl;
		}
	}

	cs.stop();
	cst->join();
}
