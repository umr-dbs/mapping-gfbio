
class BinaryReadBuffer;
static void compareBinaryReadBuffers(const BinaryReadBuffer &a, const BinaryReadBuffer &b);


#include "util/binarystream.h"
#include "util/make_unique.h"

#include "datatypes/raster.h"
#include "datatypes/attributes.h"

#include "cache/priv/shared.h"
#include "cache/priv/requests.h"
#include "cache/priv/redistribution.h"
#include "cache/priv/cache_stats.h"

#include <gtest/gtest.h>

#include <unistd.h>
#include <fcntl.h>


template<typename T>
std::unique_ptr<BinaryReadBuffer> getSerializedBuffer(T &&obj) {
	auto stream = BinaryStream::makePipe();
	BinaryWriteBuffer wb;
	wb.write(obj, true);
	stream.write(wb);

	auto rb = make_unique<BinaryReadBuffer>();
	stream.read(*rb);

	return rb;
}

template<typename T>
void checkSerializationConstructor( const T &obj ) {
	auto buf1 = getSerializedBuffer(obj);
	T obj2(*buf1);
	auto buf2 = getSerializedBuffer(obj2);
	compareBinaryReadBuffers(*buf1,*buf2);
}

static void compareBinaryReadBuffers(const BinaryReadBuffer &a, const BinaryReadBuffer &b) {
	// Check if the first buffer was completely read on deserialization
	EXPECT_EQ(a.size_read, a.size_total);
	// Now check if the buffers are identical
	EXPECT_EQ(a.getPayloadSize(), b.getPayloadSize());
	EXPECT_EQ(a.buffer.size(), b.buffer.size());
	for (size_t i = 0;i<a.buffer.size();i++)
		EXPECT_EQ(a.buffer[i], b.buffer[i]);
}

TEST(Serialization, SpatioTemporalReference) {
	SpatioTemporalReference ref1(
		SpatialReference(EPSG_LATLON, -180, -90, 180, 90),
		TemporalReference(TIMETYPE_UNIX, 0, 1)
	);
	auto buf1 = getSerializedBuffer(ref1);

	SpatioTemporalReference ref2(*buf1);
	auto buf2 = getSerializedBuffer(ref2);

	compareBinaryReadBuffers(*buf1, *buf2);
}

TEST(Serialization, Raster) {
	DataDescription dd(GDT_Byte, Unit::unknown());
	SpatioTemporalReference stref(SpatialReference::unreferenced(), TemporalReference::unreferenced());
	auto raster1 = GenericRaster::create(dd, stref, 200, 20, 1, GenericRaster::Representation::CPU);
	raster1->clear(0);
	raster1->printCentered(2, "Test-string on a raster");
	auto buf1 = getSerializedBuffer(*raster1);

	auto raster2 = GenericRaster::deserialize(*buf1);
	auto buf2 = getSerializedBuffer(*raster2);

	compareBinaryReadBuffers(*buf1, *buf2);
}

TEST(Serialization, AttributeMaps) {
	AttributeMaps attr1;
	attr1.setTextual("question", "6*7");
	attr1.setNumeric("answer", 42);
	attr1.setTextual("key", "value");
	attr1.setNumeric("keycount", 4);
	auto buf1 = getSerializedBuffer(attr1);

	AttributeMaps attr2(*buf1);
	auto buf2 = getSerializedBuffer(attr2);

	compareBinaryReadBuffers(*buf1, *buf2);
}

TEST(Serialization, AttributeArrays) {
	const size_t count = 100;
	AttributeArrays attr1;
	auto &tex = attr1.addTextualAttribute("name", Unit::unknown());
	for (size_t i=0;i<count;i++)
		tex.set(i, "test");
	auto &num = attr1.addNumericAttribute("value", Unit::unknown());
	for (size_t i=0;i<count;i++)
		num.set(i, i+1);
	attr1.validate(count);
	auto buf1 = getSerializedBuffer(attr1);
	AttributeArrays attr2(*buf1);
	auto buf2 = getSerializedBuffer(attr2);

	compareBinaryReadBuffers(*buf1, *buf2);
}

//
// CACHE/PRIV/SHARED
//

TEST(Serialization, ProfilingData) {
	ProfilingData pd;
	pd.all_cpu = 1.5;
	pd.all_gpu = 1.5;
	pd.all_io = 2048;
	checkSerializationConstructor(pd);
}

TEST(Serialization, ResolutionInfo) {
	ResolutionInfo ri;
	checkSerializationConstructor(ri);
}

TEST(Serialization, QueryCube) {
	QueryCube qc(
			SpatialReference(EPSG_LATLON, -180, -90, 180, 90),
			TemporalReference( TIMETYPE_UNIX, 0, 1e5 )
	);
	checkSerializationConstructor(qc);
}

TEST(Serialization, CacheCube) {
	CacheCube cc(
			SpatialReference(EPSG_LATLON, -180, -90, 180, 90),
			TemporalReference( TIMETYPE_UNIX, 0, 1e5 )
	);
	checkSerializationConstructor(cc);
}

TEST(Serialization, FetchInfo) {
	FetchInfo fi( 1024, ProfilingData() );
	checkSerializationConstructor(fi);
}

TEST(Serialization, CacheEntry) {
	CacheCube cc(SpatialReference(EPSG_LATLON, -180, -90, 180, 90),
			TemporalReference( TIMETYPE_UNIX, 0, 1e5 ));

	CacheEntry ce( cc, 1024, ProfilingData(), 10024373, 5 );
	checkSerializationConstructor(ce);
}

TEST(Serialization, NodeCacheKey) {
	NodeCacheKey k("key",1);
	checkSerializationConstructor(k);
}

TEST(Serialization, TypedNodeCacheKey) {
	TypedNodeCacheKey k(CacheType::RASTER,"key",1);
	checkSerializationConstructor(k);
}

TEST(Serialization, MetaCacheEntry) {
	CacheCube cc(SpatialReference(EPSG_LATLON, -180, -90, 180, 90),
				TemporalReference( TIMETYPE_UNIX, 0, 1e5 ));

	CacheEntry ce( cc, 1024, ProfilingData(), 10024373, 5 );
	TypedNodeCacheKey k(CacheType::RASTER,"key",1);
	MetaCacheEntry mce(k,ce);
	checkSerializationConstructor(mce);
}

TEST(Serialization, DeliveryResponse) {
	DeliveryResponse dr("localhost",4711,1);
	checkSerializationConstructor(dr);
}

TEST(Serialization, CacheRef) {
	CacheRef cr("localhost",4711,1);
	checkSerializationConstructor(cr);
}

//
// CACHE/PRIV/REQUESTS
//

TEST(Serialization, BaseRequest) {
	QueryRectangle qr(
			SpatialReference(EPSG_LATLON, -180, -90, 180, 90),
			TemporalReference(TIMETYPE_UNIX, 0, 1),
			QueryResolution::pixels(1024,1024)
	);

	BaseRequest br(CacheType::RASTER,"key",qr);
	checkSerializationConstructor(br);
}

TEST(Serialization, DeliveryRequest) {
	QueryRectangle qr(
			SpatialReference(EPSG_LATLON, -180, -90, 180, 90),
			TemporalReference(TIMETYPE_UNIX, 0, 1),
			QueryResolution::pixels(1024,1024)
	);

	DeliveryRequest dr( CacheType::RASTER,"key",qr,1);
	checkSerializationConstructor(dr);
}

TEST(Serialization, PuzzleRequest) {
	QueryRectangle qr(
			SpatialReference(EPSG_LATLON, -180, -90, 180, 90),
			TemporalReference(TIMETYPE_UNIX, 0, 1),
			QueryResolution::pixels(1024,1024)
	);

	std::vector<Cube<3>> remainder{ Cube3(0,1,0,1,0,1), Cube3(1,2,1,2,1,2) };
	std::vector<CacheRef> refs{ CacheRef("localhost",4711,1), CacheRef("localhost",4711,1) };

	PuzzleRequest pr( CacheType::RASTER,"key",qr, remainder, refs );
	checkSerializationConstructor(pr);
}


//
// CACHE/PRIV/REDISTRIBUTION
//


TEST(Serialization, ReorgMoveResult) {
	ReorgMoveResult rmr( CacheType::RASTER, "key", 1, 1, 2, 1 );
	checkSerializationConstructor(rmr);
}

TEST(Serialization, ReorgMoveItem) {
	ReorgMoveItem rmi(CacheType::RASTER, "key", 1, 1, "localhost", 4711 );
	checkSerializationConstructor(rmi);
}

TEST(Serialization, ReorgDescription) {
	ReorgDescription rd;

	rd.add_removal( TypedNodeCacheKey(CacheType::RASTER, "key", 1) );
	rd.add_removal( TypedNodeCacheKey(CacheType::RASTER, "key", 2) );

	rd.add_move( ReorgMoveItem(CacheType::RASTER, "key", 1, 3, "localhost", 4711 ) );
	rd.add_move( ReorgMoveItem(CacheType::RASTER, "key", 1, 4, "localhost", 4711 ) );

	checkSerializationConstructor(rd);
}

//
// CACHE/PRIV/CACHE_STATS
//

TEST(Serialization, NodeEntryStats) {
	NodeEntryStats nes(1,101238021,3);
	checkSerializationConstructor(nes);
}

TEST(Serialization, HandshakeEntry) {
	CacheCube cc(SpatialReference(EPSG_LATLON, -180, -90, 180, 90),
				TemporalReference( TIMETYPE_UNIX, 0, 1e5 ));

	CacheEntry ce( cc, 1024, ProfilingData(), 10024373, 5 );
	HandshakeEntry hse( 1, ce );
	checkSerializationConstructor(hse);
}

TEST(Serialization, CacheUsage) {
	CacheUsage cu(CacheType::RASTER, 4096, 2048 );
	checkSerializationConstructor(cu);
}

TEST(Serialization, CacheStats) {
	CacheStats cs(CacheType::RASTER,4096,2048);
	cs.add_item("key1", NodeEntryStats(1,101238021,3));
	cs.add_item("key1", NodeEntryStats(2,101238021,3));
	cs.add_item("key2", NodeEntryStats(3,101238021,3));
	cs.add_item("key2", NodeEntryStats(4,101238021,3));
	checkSerializationConstructor(cs);
}

TEST(Serialization, CacheHandshake) {
	CacheCube cc(SpatialReference(EPSG_LATLON, -180, -90, 180, 90),
				 TemporalReference( TIMETYPE_UNIX, 0, 1e5 ));

	CacheHandshake ch(CacheType::RASTER,4096,2048);
	ch.add_item("key1", HandshakeEntry(1, CacheEntry(cc, 1024, ProfilingData(), 10024373,5 ) ) );
	ch.add_item("key1", HandshakeEntry(2, CacheEntry(cc, 1024, ProfilingData(), 10024373,5 ) ) );
	ch.add_item("key2", HandshakeEntry(3, CacheEntry(cc, 1024, ProfilingData(), 10024373,5 ) ) );
	ch.add_item("key2", HandshakeEntry(4, CacheEntry(cc, 1024, ProfilingData(), 10024373,5 ) ) );
	checkSerializationConstructor(ch);
}

TEST(Serialization, QueryStats) {
	QueryStats qs;
	qs.misses = 1;
	qs.multi_local_hits = 2;
	qs.multi_local_partials = 3;
	qs.multi_remote_hits = 4;
	qs.multi_remote_partials = 5;
	qs.single_local_hits = 6;
	qs.single_remote_hits = 7;
	checkSerializationConstructor(qs);
}

TEST(Serialization, NodeStats) {

	QueryStats qs;
	qs.misses = 1;
	qs.multi_local_hits = 2;
	qs.multi_local_partials = 3;
	qs.multi_remote_hits = 4;
	qs.multi_remote_partials = 5;
	qs.single_local_hits = 6;
	qs.single_remote_hits = 7;

	CacheStats cs(CacheType::RASTER,4096,2048);
	cs.add_item("key1", NodeEntryStats(1,101238021,3));
	cs.add_item("key1", NodeEntryStats(2,101238021,3));
	cs.add_item("key2", NodeEntryStats(3,101238021,3));
	cs.add_item("key2", NodeEntryStats(4,101238021,3));

	NodeStats ns(qs, std::vector<CacheStats>{cs,cs});
	checkSerializationConstructor(ns);
}

TEST(Serialization, NodeHandshake) {

	CacheCube cc(SpatialReference(EPSG_LATLON, -180, -90, 180, 90),
					 TemporalReference( TIMETYPE_UNIX, 0, 1e5 ));

	CacheHandshake ch(CacheType::RASTER,4096,2048);
	ch.add_item("key1", HandshakeEntry(1, CacheEntry(cc, 1024, ProfilingData(), 10024373,5 ) ) );
	ch.add_item("key1", HandshakeEntry(2, CacheEntry(cc, 1024, ProfilingData(), 10024373,5 ) ) );
	ch.add_item("key2", HandshakeEntry(3, CacheEntry(cc, 1024, ProfilingData(), 10024373,5 ) ) );
	ch.add_item("key2", HandshakeEntry(4, CacheEntry(cc, 1024, ProfilingData(), 10024373,5 ) ) );

	NodeHandshake nhs(4711, std::vector<CacheHandshake>{ch,ch});
	checkSerializationConstructor(nhs);
}
