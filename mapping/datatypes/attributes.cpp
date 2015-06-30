
#include "util/exceptions.h"
#include "datatypes/attributes.h"
#include "util/binarystream.h"

#include <limits>

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
DirectMetadata<T>::DirectMetadata(BinaryStream &stream) {
	fromStream(stream);
}

template<typename T>
void DirectMetadata<T>::fromStream(BinaryStream &stream) {
	size_t count;
	stream.read(&count);
	for (size_t i=0;i<count;i++) {
		std::string key;
		stream.read(&key);
		T value;
		stream.read(&value);
		data[key] = value;
	}
}

template<typename T>
void DirectMetadata<T>::toStream(BinaryStream &stream) const {
	size_t count = data.size();
	stream.write(count);
	for (auto &e : data) {
		stream.write(e.first);
		stream.write(e.second);
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
	if (data.count(key) == 0) {
		//data[key] = std::vector<T>();
		throw MetadataException("Metadata with key "+key+" does not exist. Call addVector() first.");
	}

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

template<typename T> struct defaultvalue {
};

template <> struct defaultvalue<double> {
	static const double value;
};
const double defaultvalue<double>::value = std::numeric_limits<double>::quiet_NaN();

template <> struct defaultvalue<std::string> {
	static const std::string value;
};
const std::string defaultvalue<std::string>::value = "";

template<typename T>
std::vector<T> &MetadataArrays<T>::addVector(const std::string &key, size_t capacity) {
	if (data.count(key) > 0)
		throw MetadataException("Metadata with key "+key+" already exists");
	data[key] = std::vector<T>(capacity, defaultvalue<T>::value);
	return data.at(key);
}
template<typename T>
std::vector<T> &MetadataArrays<T>::addEmptyVector(const std::string &key, size_t reserve) {
	if (data.count(key) > 0)
		throw MetadataException("Metadata with key "+key+" already exists");
	data[key] = std::vector<T>();
	auto &vec = data.at(key);
	if (reserve > 0)
		vec.reserve(reserve);
	return vec;
}

template<typename T>
std::vector<T> &MetadataArrays<T>::getVector(const std::string &key) {
	return data.at(key);
}

template<typename T>
const std::vector<T> &MetadataArrays<T>::getVector(const std::string &key) const {
	return data.at(key);
}

template<typename T>
MetadataArrays<T>::MetadataArrays(BinaryStream &stream) {
	fromStream(stream);
}

template<typename T>
void MetadataArrays<T>::fromStream(BinaryStream &stream) {
	size_t keycount;
	stream.read(&keycount);
	for (size_t k=0;k<keycount;k++) {
		std::string key;
		stream.read(&key);
		size_t vecsize;
		stream.read(&vecsize);
		std::vector<T> vec(vecsize);
		for (size_t i=0;i<vecsize;i++) {
			T value;
			stream.read(&value);
			vec.push_back(value);
		}
		data[key] = std::move(vec);
	}
}

template<typename T>
void MetadataArrays<T>::toStream(BinaryStream &stream) const {
	size_t keycount = data.size();
	stream.write(keycount);
	for (auto &e : data) {
		auto &key = e.first;
		stream.write(key);
		auto &vec = e.second;
		auto vecsize = vec.size();
		stream.write(vecsize);
		for (size_t i=0;i<vecsize;i++)
			stream.write(vec[i]);
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
