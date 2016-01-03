
#include <gtest/gtest.h>
#include "cache/common.h"
#include "cache/node/node_cache.h"
#include "test/unittests/cache/util.h"
#include "operators/operator.h"
#include "util/configuration.h"
#include "util/make_unique.h"


TEST(STCacheTest,SimpleTest) {
	Configuration::loadFromDefaultPaths();

	std::string timestr("2010-06-06T18:00:00.000Z");
	epsg_t epsg = EPSG_LATLON;
	uint32_t width = 256, height = 256;
	time_t timestamp = parseIso8601DateTime(timestr);
	std::string sem_id = "TEST";

	std::string bboxes[] = {
		std::string("45,-180,67.5,-157.5"),
		std::string("45,-157.5,67.5,-135"),
		std::string("45,-135,67.5,-112.5"),
		std::string("45,-112.5,67.5,-90")
	};
	double bbox[4];

	NodeCache<GenericRaster> cache(CacheType::RASTER, 114508*2 + 17);

	DataDescription dd( GDALDataType::GDT_Byte, Unit::unknown() );


	for ( int i = 0; i < 4; i++ ) {
		parseBBOX(bbox, bboxes[i], epsg, false);
		QueryRectangle qr(
			SpatialReference(epsg, bbox[0], bbox[1], bbox[2], bbox[3]),
			TemporalReference(TIMETYPE_UNIX, timestamp, timestamp),
			QueryResolution::pixels(width, height)
		);
		QueryProfiler qp;
		CacheQueryResult<uint64_t> qres = cache.query(sem_id,qr);
		printf("%s", qres.to_string().c_str());
		ASSERT_TRUE( qres.has_remainder() );
		auto res = GenericRaster::create(dd,qr,width,height);
		CacheEntry meta( CacheCube(*res), 10, 1.0 );
		cache.put(sem_id, res, meta);
		qres = cache.query(sem_id,qr);
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
	NopCacheManager ncm;
	CacheManager::init( &ncm );
	NodeCache<GenericRaster> cache(CacheType::RASTER, 5 * 1024 * 1024);
	std::string sem_id = "a";

	DataDescription dd(GDT_Byte, Unit::unknown());


	auto r1 = createRaster(0,1,0,1);
	auto r2 = createRaster(0,1,1,2);
	auto r3 = createRaster(1,2,0,1);

	cache.put( sem_id, r1, CacheEntry( CacheCube(*r1), 10, 1.0) );
	cache.put( sem_id, r2, CacheEntry( CacheCube(*r2), 10, 1.0) );
	cache.put( sem_id, r3, CacheEntry( CacheCube(*r3), 10, 1.0) );

	QueryRectangle qrect(
		SpatialReference(EPSG_LATLON, 0, 0, 2, 2),
		TemporalReference(TIMETYPE_UNIX, 10, 10),
		QueryResolution::pixels(2, 2)
	);
	CacheQueryResult<uint64_t> qr = cache.query(sem_id, qrect);

	ASSERT_TRUE( qr.has_remainder() );


	auto &rem = qr.remainder.at(0);
	printf("Remainder:\n%s\n", rem.to_string().c_str());

	ASSERT_EQ( rem, Cube3( 1,2,1,2,0,100) );

	auto r4 = createRaster(1,2,1,2);
	cache.put(sem_id,r4, CacheEntry( CacheCube(*r4), 10, 1.0) );

	qr = cache.query( sem_id, qrect );
	ASSERT_FALSE(qr.has_remainder());
	ASSERT_EQ( 4, qr.keys.size() );


}


