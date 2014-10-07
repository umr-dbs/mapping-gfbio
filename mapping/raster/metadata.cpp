
#include "raster/exceptions.h"
#include "raster/metadata.h"
#include "util/socket.h"


/*
 * DirectMetadata
 * Stores key/value-pairs directly.
 */
template<typename T>
DirectMetadata<T>::DirectMetadata() {
}

template<typename T>
DirectMetadata<T>::~DirectMetadata() {
}

template<typename T>
void DirectMetadata<T>::set(const std::string &key, const T &value) {
	if (data.count(key) > 0)
		throw MetadataException("Tried to set metadata '" + key + "' that's already been set.");
	data[key] = value;
}

template<typename T>
const T &DirectMetadata<T>::get(const std::string &key) const {
	if (data.count(key) < 1)
		throw MetadataException("DirectMetadata::get(): No value stored for key '" + key + "'");
	return data.at(key);
}

template<typename T>
const T &DirectMetadata<T>::get(const std::string &key, const T &defaultvalue) const {
	if (data.count(key) < 1)
		return defaultvalue;
	return data.at(key);
}

template<typename T>
DirectMetadata<T>::DirectMetadata(Socket &socket) {
	fromSocket(socket);
}

template<typename T>
void DirectMetadata<T>::fromSocket(Socket &socket) {
	size_t count;
	socket.read(&count);
	for (size_t i=0;i<count;i++) {
		std::string key;
		socket.read(&key);
		T value;
		socket.read(&value);
		data[key] = value;
	}
}

template<typename T>
void DirectMetadata<T>::toSocket(Socket &socket) const {
	size_t count = data.size();
	socket.write(count);
	for (auto &e : data) {
		socket.write(e.first);
		socket.write(e.second);
	}
}



/**
 * MetadataArrays
 */
template<typename T>
MetadataArrays<T>::MetadataArrays() {
}

template<typename T>
MetadataArrays<T>::~MetadataArrays() {
}

template<typename T>
void MetadataArrays<T>::set(size_t idx, const std::string &key, const T &value) {
	if (data.count(key) == 0)
		data[key] = std::vector<T>();

	auto &vec = data[key];
	if (idx == vec.size()) {
		vec.push_back(value);
		return;
	}
	if (vec.size() < idx+1)
		vec.resize(idx+1);
	vec[idx] = value;
}

template<typename T>
const T &MetadataArrays<T>::get(size_t idx, const std::string &key) const {
	return data.at(key).at(idx);
}

template<typename T>
const T &MetadataArrays<T>::get(size_t idx, const std::string &key, const T &defaultvalue) const {
	try {
		return data.at(key).at(idx);
	}
	catch (std::out_of_range &e) {
		return defaultvalue;
	}
}

template<typename T>
std::vector<T> &MetadataArrays<T>::addVector(const std::string &key, size_t capacity) {
	if (data.count(key) > 0)
		throw MetadataException("Metadata with key "+key+" already exists");
	data[key] = std::vector<T>(capacity);
	return data.at(key);
}

template<typename T>
std::vector<T> &MetadataArrays<T>::getVector(const std::string &key) {
	return data.at(key);
}

template<typename T>
MetadataArrays<T>::MetadataArrays(Socket &socket) {
	fromSocket(socket);
}

template<typename T>
void MetadataArrays<T>::fromSocket(Socket &socket) {
	size_t keycount;
	socket.read(&keycount);
	for (size_t k=0;k<keycount;k++) {
		std::string key;
		socket.read(&key);
		size_t vecsize;
		socket.read(&vecsize);
		std::vector<T> vec(vecsize);
		for (size_t i=0;i<vecsize;i++) {
			T value;
			socket.read(&value);
			vec.push_back(value);
		}
		data[key] = std::move(vec);
	}
}

template<typename T>
void MetadataArrays<T>::toSocket(Socket &socket) const {
	size_t keycount = data.size();
	socket.write(keycount);
	for (auto &e : data) {
		auto &key = e.first;
		socket.write(key);
		auto &vec = e.second;
		auto vecsize = vec.size();
		socket.write(vecsize);
		for (size_t i=0;i<vecsize;i++)
			socket.write(vec[i]);
	}
}

template<typename T>
std::vector<std::string> MetadataArrays<T>::getKeys() const {
	std::vector<std::string> keys;
	for (auto keyValue : data)
		keys.push_back(keyValue.first);
	return keys;
}



// Instantiate as required
template class DirectMetadata<std::string>;
template class DirectMetadata<double>;

template class MetadataArrays<std::string>;
template class MetadataArrays<double>;
