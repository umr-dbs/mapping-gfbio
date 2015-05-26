/*
 * server.cpp
 *
 *  Created on: 18.05.2015
 *      Author: mika
 */

#include <gtest/gtest.h>
#include "test/unittests/cache/util.h"
#include "util/configuration.h"
#include "operators/operator.h"
#include "cache/cache.h"
#include "cache/server.h"

#include <iostream>

TEST(CacheServerTest,SimpleTest) {
	Configuration::loadFromDefaultPaths();

	std::unique_ptr<CacheManager> mgrImpl( new DefaultCacheManager(5 * 1024 * 1024) );
	//std::unique_ptr<CacheManager> mgrImpl( new NopCacheManager() );

	CacheManager::init( mgrImpl );

	std::string json = "{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}";
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
	std::unique_ptr<std::thread> cst = cs.runAsync();

	for ( int i = 0; i < 4; i++ ) {
		parseBBOX(bbox, bboxes[i], epsg, false);
		QueryRectangle qr(timestamp, bbox[0], bbox[1], bbox[2], bbox[3], width, height, epsg);


		uint8_t cmd = CacheServer::COMMAND_GET_RASTER;
		uint8_t querymode = 1;
		UnixSocket sock("localhost",12346);
		BinaryStream &stream = sock;

		stream.write(cmd);
		qr.toStream(stream);
		stream.write(json);
		stream.write(querymode);

		uint8_t resp_code;
		stream.read(&resp_code);

		uint8_t expected = CacheServer::RESPONSE_OK;
		ASSERT_EQ( expected, resp_code );
	}

	cs.stop();
	cst->join();
}
