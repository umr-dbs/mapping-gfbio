
#include <gtest/gtest.h>
#include "cache/common.h"
#include "cache/node/node_cache.h"
#include "test/unittests/cache/util.h"
#include "operators/operator.h"
#include "util/configuration.h"
#include "util/make_unique.h"


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

	CacheManager::init( make_unique<NopCacheManager>(), make_unique<CacheAll>() );

	NodeRasterCache cache(114508*2 + 17);


	for ( int i = 0; i < 4; i++ ) {
		parseBBOX(bbox, bboxes[i], epsg, false);
		QueryRectangle qr(
			SpatialReference(epsg, bbox[0], bbox[1], bbox[2], bbox[3]),
			TemporalReference(TIMETYPE_UNIX, timestamp, timestamp),
			QueryResolution::pixels(width, height)
		);
		QueryProfiler qp;
		CacheQueryResult<uint64_t> qres = cache.query(op->getSemanticId(),qr);
		printf("%s", qres.to_string().c_str());
		ASSERT_TRUE( qres.has_remainder() );
		auto res = op->getCachedRaster(qr,qp);
		cache.put(op->getSemanticId(), res);
		qres = cache.query(op->getSemanticId(),qr);
		ASSERT_TRUE( qres.has_hit() );
		ASSERT_FALSE( qres.has_remainder() );
	}
}

std::unique_ptr<GenericRaster> createRaster( double x1, double x2, double y1, double y2 ) {
	DataDescription dd(GDT_Byte, Unit::unknown());
	SpatioTemporalReference stref(
		SpatialReference(EPSG_LATLON,x1,y1,x2,y2),
		TemporalReference(TIMETYPE_UNIX,0,100)
	);
	return GenericRaster::create(dd,stref,x2-x1,y2-y1,0,GenericRaster::Representation::CPU);
}


TEST(STCacheTest,TestQuery) {
	NodeRasterCache cache(5 * 1024 * 1024);
	std::string sem_id = "a";

	DataDescription dd(GDT_Byte, Unit::unknown());


	auto r1 = createRaster(0,1,0,1);
	auto r2 = createRaster(0,1,1,2);
	auto r3 = createRaster(1,2,0,1);

	cache.put( sem_id, r1 );
	cache.put( sem_id, r2 );
	cache.put( sem_id, r3 );

	QueryRectangle qrect(
		SpatialReference(EPSG_LATLON, 0, 0, 2, 2),
		TemporalReference(TIMETYPE_UNIX, 10, 10),
		QueryResolution::pixels(2, 2)
	);
	CacheQueryResult<uint64_t> qr = cache.query(sem_id, qrect);

	ASSERT_TRUE( qr.has_remainder() );



	geos::geom::CoordinateSequence *coords = qr.remainder->getEnvelope()->getCoordinates();

	double x1 = DoubleInfinity,
		   x2 = DoubleNegInfinity,
		   y1 = DoubleInfinity,
		   y2 = DoubleNegInfinity;

	for ( size_t i = 0; i < coords->size(); i++ ) {
		auto c = coords->getAt(i);
		x1 = std::min(c.x,x1);
		y1 = std::min(c.y,y1);
		x2 = std::max(c.x,x2);
		y2 = std::max(c.y,y2);
	}

	ASSERT_DOUBLE_EQ( 1.01, x1 );
	ASSERT_DOUBLE_EQ( 1.01, y1 );
	ASSERT_DOUBLE_EQ( 2.0 , x2 );
	ASSERT_DOUBLE_EQ( 2.0 , y2 );

	auto r4 = createRaster(1,2,1,2);
	cache.put(sem_id,r4);

	qr = cache.query( sem_id, qrect );
	ASSERT_FALSE(qr.has_remainder());
	ASSERT_EQ( 4, qr.keys.size() );


}


