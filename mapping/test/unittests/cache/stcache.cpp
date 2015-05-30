#include <gtest/gtest.h>
#include "test/unittests/cache/util.h"
#include "cache/cache.h"
#include "operators/operator.h"
#include "util/configuration.h"


TEST(STCacheTest,SimpleTest) {
	Configuration::loadFromDefaultPaths();

	std::string json = "{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}";
	std::string timestr("2010-06-06T18:00:00.000Z");
	epsg_t epsg = EPSG_LATLON;
	uint32_t width = 256, height = 256;
	time_t timestamp = parseIso8601DateTime(timestr);
	GenericOperator *op = GenericOperator::fromJSON( json ).release();


	std::string bboxes[] = {
		std::string("45,-180,67.5,-157.5"),
		std::string("45,-157.5,67.5,-135"),
		std::string("45,-135,67.5,-112.5"),
		std::string("45,-112.5,67.5,-90")
	};
	double bbox[4];

	std::unique_ptr<CacheManager> impl( new NopCacheManager() );
	CacheManager::init( impl );

	RasterCache cache(114508*2 + 17);


	for ( int i = 0; i < 4; i++ ) {
		parseBBOX(bbox, bboxes[i], epsg, false);
		QueryRectangle qr(timestamp, bbox[0], bbox[1], bbox[2], bbox[3], width, height, epsg);
		QueryProfiler qp;
		try {
			cache.get(op->getSemanticId(),qr);
			FAIL();
		} catch ( NoSuchElementException &nse ) {
			auto res = op->getCachedRaster(qr,qp);
			cache.put(op->getSemanticId(), res);
			auto cached = cache.get(op->getSemanticId(),qr);
		}
	}
}
