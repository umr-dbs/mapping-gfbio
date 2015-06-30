/*
 * LinkedHashMap.h
 *
 *  Created on: 11.05.2015
 *      Author: mika
 */

#ifndef LINKEDHASHMAP_H_
#define LINKEDHASHMAP_H_

#include <unordered_map>
#include "util/exceptions.h"

template<typename K, typename V>
class LinkedHashMap {
protected:
	class _LinkedEntry {
	public:
		_LinkedEntry(const K &key, const V &value, _LinkedEntry *prev = nullptr,
				_LinkedEntry *next = nullptr) :
				key(key), value(value), prev(prev), next(next) {
		}
		;
		K key;
		V value;
		_LinkedEntry *prev, *next;

	};

public:
	LinkedHashMap(bool accessOrder = false) :
			accessOrder(accessOrder), first(nullptr), last(nullptr) {};
	~LinkedHashMap();
	void put(const K &key, const V &value);
	V get(const K &key);
	V removeEldestEntry();
	void remove(const K &key);
	size_t size();

private:
	bool accessOrder;
	std::unordered_map<K, _LinkedEntry*> map;
	_LinkedEntry *first, *last;
};


template<typename K, typename V>
LinkedHashMap<K, V>::~LinkedHashMap() {

	_LinkedEntry *tmp = nullptr;
	for ( _LinkedEntry *e = first; e != nullptr; ) {
		tmp = e;
		e = e->next;
		delete tmp;
	}
}


template<typename K, typename V>
void LinkedHashMap<K, V>::put(const K &key, const V &value) {
	_LinkedEntry *link = new _LinkedEntry(key,value, nullptr, first);
	if (link->next != nullptr) {
		link->next->prev = link;
	}
	first = link;
	if (last == nullptr)
		last = link;
	map.emplace( key, link );
}

template<typename K, typename V>
size_t LinkedHashMap<K, V>::size() {
	return map.size();
}

template<typename K, typename V>
V LinkedHashMap<K, V>::get(const K &key) {

	auto got = map.find(key);

	if (got == map.end())
		return nullptr;
	else if (accessOrder) {
		_LinkedEntry *e = got->second;
		// We are not the first element
		if (e != first ) {
			e->prev->next = e->next;

			if (e->next != nullptr)
				e->next->prev = e->prev;
			else
				last = e->prev;

			e->prev = nullptr;
			e->next = first;
			first->prev = e;
			first = e;
		}
		return e->value;
	} else {
		return got->second->value;
	}
}

template<typename K, typename V>
void LinkedHashMap<K, V>::remove(const K &key) {
	auto got = map.find(key);

	if (got != map.end()) {
		_LinkedEntry *e = got->second;

		if ( e == first && e != last) {
			first = e->next;
			first->prev = nullptr;
		}
		else if ( e == last ) {
			last = e->prev;
			last->next = nullptr;
		}
		else if ( e == first ) {
			last = first = nullptr;
		}
		else {
			e->prev->next = e->next;
			e->next->prev = e->prev;
		}
		map.erase(got);
		delete e;
	}
}

template<typename K, typename V>
V LinkedHashMap<K, V>::removeEldestEntry() {
	if ( last != nullptr ) {
		_LinkedEntry *tmp = last;
		V res = last->value;
		if ( last != first ) {
			last = last->prev;
			last->next = nullptr;
		}
		else {
			last = first = nullptr;
		}
		map.erase(tmp->key);
		delete tmp;
		return res;
	}
	throw OperatorException("Cannot remove eldest element of empty map");
}

#endif /* LINKEDHASHMAP_H_ */
