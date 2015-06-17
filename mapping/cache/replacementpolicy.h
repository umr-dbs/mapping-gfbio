/*
 * replacementpolicy.h
 *
 *  Created on: 15.05.2015
 *      Author: mika
 */

#ifndef REPLACEMENTPOLICY_H_
#define REPLACEMENTPOLICY_H_

#include "linkedhashmap.h"
#include <unordered_map>

template <typename T> class STCacheEntry;

template <typename T>
class ReplacementPolicy {
public:
	virtual ~ReplacementPolicy() {};
	virtual void inserted( const std::shared_ptr<STCacheEntry<T>> &entry ) = 0;
	virtual void accessed( const std::shared_ptr<STCacheEntry<T>> &entry ) = 0;
	virtual std::shared_ptr<STCacheEntry<T>> evict() = 0;
};

template <typename T>
class LRUPolicy : public ReplacementPolicy<T> {
public:
	LRUPolicy();
	virtual ~LRUPolicy();
	virtual void inserted( const std::shared_ptr<STCacheEntry<T>> &entry );
	virtual void accessed( const std::shared_ptr<STCacheEntry<T>> &entry );
	virtual std::shared_ptr<STCacheEntry<T>> evict();
private:
	LinkedHashMap<std::shared_ptr<STCacheEntry<T>>,std::shared_ptr<STCacheEntry<T>>> map;
};

template <typename T> LRUPolicy<T>::LRUPolicy() : map(true) {};
template <typename T> LRUPolicy<T>::~LRUPolicy() {};

template <typename T>
void LRUPolicy<T>::inserted( const std::shared_ptr<STCacheEntry<T>> &entry ) {
	map.put(entry,entry);
}

template <typename T>
void LRUPolicy<T>::accessed( const std::shared_ptr<STCacheEntry<T>> &entry ) {
	map.get(entry);
}

template <typename T>
std::shared_ptr<STCacheEntry<T>> LRUPolicy<T>::evict() {
	return map.removeEldestEntry();
}

#endif /* REPLACEMENTPOLICY_H_ */
