#include <gtest/gtest.h>
#include "cache/index/indexserver.h"
#include "cache/index/index_cache.h"
#include "cache/index/querymanager.h"
#include "cache/index/reorg_strategy.h"

TEST(Locking,CacheLocksTest) {

	CacheLocks locks;
	IndexCacheKey key("Test",1,1);
	CacheLocks::Lock l( CacheType::POINT, key );

	locks.add_lock(l);
	ASSERT_TRUE(  locks.is_locked(l) );
	ASSERT_TRUE(  locks.is_locked( CacheType::POINT, key) );
	ASSERT_FALSE( locks.is_locked( CacheType::RASTER, key) );

	locks.add_lock(l);
	ASSERT_TRUE(  locks.is_locked(l) );

	locks.remove_lock(l);
	ASSERT_TRUE(  locks.is_locked(l) );

	locks.remove_lock(l);
	ASSERT_FALSE(  locks.is_locked(l) );
}

QueryRectangle create_query( const SpatialReference &sref ) {
	return QueryRectangle(
			sref,
			TemporalReference(TIMETYPE_UNIX,1,2),
			QueryResolution::none()
	);
}

std::shared_ptr<IndexCacheEntry> create_entry( uint32_t node_id, const std::string sem_id, uint64_t e_id, const SpatialReference &sref ) {
	CacheCube cc( sref, TemporalReference(TIMETYPE_UNIX,0,1e10));
	MetaCacheEntry ref( CacheType::POINT, sem_id, e_id, CacheEntry( cc, 10, ProfilingData() ) );
	return std::shared_ptr<IndexCacheEntry>( new IndexCacheEntry( node_id, ref ) );
}

TEST(Locking,MgrLocks) {
	NodeHandshake hs(4711, std::vector<CacheHandshake>{} );
	std::shared_ptr<Node> n( new Node(1,"fakehost",hs) );
	std::map<uint32_t,std::shared_ptr<Node>> node_map;
	std::string sem_id = "test";
	node_map.emplace(n->id,n);
	IndexCacheManager ic( "capacity","lru" );

	QueryManager mgr(ic,node_map);

	auto &c = ic.get_cache(CacheType::POINT);

	auto e1 = create_entry(n->id, sem_id, 1, SpatialReference(EPSG_LATLON, 0,0, 10, 10) );
	auto e2 = create_entry(n->id, sem_id, 2, SpatialReference(EPSG_LATLON, 10,0, 20, 10) );
	auto e3 = create_entry(n->id, sem_id, 1, SpatialReference(EPSG_LATLON, 0,10, 10, 20) );
	auto e4 = create_entry(n->id, sem_id, 2, SpatialReference(EPSG_LATLON, 10,10, 20, 20) );
	c.put(e1);
	c.put(e2);
	c.put(e3);
	c.put(e4);


	// Query full hit
	mgr.add_request(1, BaseRequest( CacheType::POINT, sem_id, create_query(SpatialReference(EPSG_LATLON, 0,0, 10, 10)) ) );
	ASSERT_TRUE( mgr.is_locked( CacheType::POINT, IndexCacheKey(sem_id,1,1) ) );
	mgr.handle_client_abort(1);
	ASSERT_FALSE( mgr.is_locked( CacheType::POINT, IndexCacheKey(sem_id,1,1) ) );

	// Check shared locks
	// Delivery
	mgr.add_request(1,BaseRequest( CacheType::POINT, sem_id, create_query(SpatialReference(EPSG_LATLON, 0,0, 10, 10)) ));
	ASSERT_TRUE( mgr.is_locked( CacheType::POINT, IndexCacheKey(sem_id,1,1) ) );
	// Puzzle
	mgr.add_request(2,BaseRequest( CacheType::POINT, sem_id, create_query(SpatialReference(EPSG_LATLON, 0,0, 20, 10)) ));
	ASSERT_TRUE( mgr.is_locked( CacheType::POINT, IndexCacheKey(sem_id,1,1) ) );
	ASSERT_TRUE( mgr.is_locked( CacheType::POINT, IndexCacheKey(sem_id,1,2) ) );

	mgr.handle_client_abort(2);
	ASSERT_TRUE( mgr.is_locked( CacheType::POINT, IndexCacheKey(sem_id,1,1) ) );
	ASSERT_FALSE( mgr.is_locked( CacheType::POINT, IndexCacheKey(sem_id,1,2) ) );

	mgr.handle_client_abort(1);
	ASSERT_FALSE( mgr.is_locked( CacheType::POINT, IndexCacheKey(sem_id,1,1) ) );

}

