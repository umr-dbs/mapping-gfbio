/*
 * sizeutil.h
 *
 *  Created on: 27.11.2015
 *      Author: mika
 */

#ifndef SIZEUTIL_H_
#define SIZEUTIL_H_

#include <string>
#include <type_traits>
#include <vector>
#include <map>

/**
 * Utility to estimate the size of elements in bytes.
 * STL-Overhead taken from:
 * http://info.prelert.com/blog/stl-container-memory-usage
 */

class GenericPlot;
class Coordinate;

class SizeUtil {
public:

	template <typename T>
	static typename std::enable_if<!std::is_class<T>::value,size_t>::type get_byte_size(const T& s);

	template <typename T>
	static typename std::enable_if<std::is_class<T>::value,size_t>::type get_byte_size(const T& s);

	template <typename T>
	static typename std::enable_if<!std::is_class<T>::value,size_t>::type get_vec_size(const std::vector<T>& v);

	template <typename T>
	static typename std::enable_if<std::is_class<T>::value,size_t>::type get_vec_size(const std::vector<T>& v);

	template <typename K, typename V>
	static typename std::enable_if<!std::is_class<K>::value && !std::is_class<V>::value,size_t>::type
		get_map_size(const std::map<K,V>& m);

	template <typename K, typename V>
	static typename std::enable_if<std::is_class<K>::value || std::is_class<V>::value,size_t>::type
		get_map_size(const std::map<K,V>& m);
	SizeUtil() {};
};

namespace sizeutil {
	template <typename T> struct helper {
	    static size_t get_size( const T &value) { return value.get_byte_size(); }
	};

	template <typename T> struct helper<std::vector<T>> {
		static size_t get_size( const std::vector<T> &value) { return SizeUtil::get_vec_size(value); }
	};

	template <typename K, typename V> struct helper<std::map<K,V>> {
		static size_t get_size( const std::map<K,V> &value) { return SizeUtil::get_map_size(value); }
	};

	// Specializations
	template <> struct helper<std::string> {
		static size_t get_size( const std::string &value);
	};

	template <> struct helper<std::vector<Coordinate>> {
		static size_t get_size( const std::vector<Coordinate> &value);
	};

	template <> struct helper<GenericPlot> {
		static size_t get_size( const GenericPlot &value);
	};
}

template <typename T>
inline typename std::enable_if<!std::is_class<T>::value,size_t>::type SizeUtil::get_byte_size(const T& s) {
	(void) s;
	return sizeof(T);
}

template <typename T>
inline typename std::enable_if<std::is_class<T>::value,size_t>::type SizeUtil::get_byte_size(const T& s) {
	return sizeutil::helper<T>::get_size(s);
}

template <typename T>
inline typename std::enable_if<!std::is_class<T>::value,size_t>::type SizeUtil::get_vec_size(const std::vector<T>& v) {
	return 24 + v.capacity() * sizeof(T);
}

template <typename T>
inline typename std::enable_if<std::is_class<T>::value,size_t>::type SizeUtil::get_vec_size(const std::vector<T>& v) {
	size_t res = 24;
	for ( auto &e : v )
		res += get_byte_size(e);
	res += (v.capacity()-v.size()) * sizeof(T);
	return res;
}

template <typename K, typename V>
inline typename std::enable_if<!std::is_class<K>::value && !std::is_class<V>::value,size_t>::type
	SizeUtil::get_map_size(const std::map<K,V>& m) {
	return 48 + m.size() * (32+sizeof(K)+sizeof(V));
}

template <typename K, typename V>
inline typename std::enable_if<std::is_class<K>::value || std::is_class<V>::value,size_t>::type
	SizeUtil::get_map_size(const std::map<K,V>& m) {
	size_t res = 48 + 32*m.size();
	for ( auto &p : m )
		res += get_byte_size(p.first) + get_byte_size(p.second);
	return res;
}

#endif /* SIZEUTIL_H_ */
