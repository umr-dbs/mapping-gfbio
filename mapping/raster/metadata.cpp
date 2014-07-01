
#include "raster/exceptions.h"
#include "raster/metadata.h"

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
const T &DirectMetadata<T>::get(const std::string &key) {
	if (data.count(key) < 1)
		throw MetadataException("DirectMetadata::get(): No value stored for key '" + key + "'");
	return data.at(key);
}

template<typename T>
const T &DirectMetadata<T>::get(const std::string &key, const T &defaultvalue) {
	if (data.count(key) < 1)
		return defaultvalue;
	return data.at(key);
}


/**
 * MetadataIndex
 * Stores key/index-pairs. The index is used to lookup values from IndexedMetadata
 */
template<typename T>
MetadataIndex<T>::MetadataIndex() : index_is_locked(false) {
}

template<typename T>
MetadataIndex<T>::~MetadataIndex() {
}

template<typename T>
void MetadataIndex<T>::addKey(const std::string &key) {
	if (index_is_locked)
		throw MetadataException("Cannot add keys to locked MetadataIndex.");
	if (index.count(key) > 0)
		throw MetadataException("Tried to add key to MetadataIndex that's already been added.");
	if (index.size() > 250)
		throw MetadataException("MetadataIndex is full (cannot add more than 250 entries)");
	metadata_index_t new_index = index.size();
	index[key] = new_index;
}

template<typename T>
void MetadataIndex<T>::lock() {
	index_is_locked = true;
}

template<typename T>
void MetadataIndex<T>::setValue(IndexedMetadata<T> &object, const std::string &key, const T &value) {
	if (!index_is_locked)
		throw MetadataException("The MetadataIndex needs to be lock()ed before setting values.");
	if (object.size != index.size())
		throw MetadataException("MetadataIndex::setValue on incompatible IndexedMetadata!");
	if (index.count(key) < 1)
		throw MetadataException("MetadataIndex::setValue on an unknown key");

	metadata_index_t pos = index.at(key);
	object.data[pos] = value;
}

template<typename T>
const T &MetadataIndex<T>::getValue(const IndexedMetadata<T> &object, const std::string &key) {
	if (!index_is_locked)
		throw MetadataException("The MetadataIndex needs to be lock()ed before getting values.");
	if (object.size != index.size())
		throw MetadataException("MetadataIndex::getValue on incompatible IndexedMetadata!");
	if (index.count(key) < 1)
		throw MetadataException("MetadataIndex::getValue on an unknown key");

	metadata_index_t pos = index.at(key);
	return object.data[pos];
}


/**
 * IndexedMetadata
 */
template<typename T>
IndexedMetadata<T>::IndexedMetadata(metadata_index_t size) : size(size), data(nullptr) {
	if (size > 0)
		data = new T[size];
}

template<typename T>
IndexedMetadata<T>::~IndexedMetadata() {
	delete [] data;
	data = nullptr;
}


// Copy
template<typename T>
IndexedMetadata<T>::IndexedMetadata(const IndexedMetadata<T> &imd) : size(imd.size), data(nullptr) {
	if (size > 0) {
		data = new T[size];
		for (metadata_index_t i=0;i<size;i++)
			data[i] = imd.data[i];
	}
}

template<typename T>
IndexedMetadata<T> &IndexedMetadata<T>::operator=(const IndexedMetadata<T> &imd) {
	delete [] data;
	data = nullptr;
	size = imd.size;
	if (size > 0) {
		data = new T[size];
		for (metadata_index_t i=0;i<size;i++)
			data[i] = imd.data[i];
	}
	return *this;
}

// Move
template<typename T>
IndexedMetadata<T>::IndexedMetadata(IndexedMetadata<T> &&imd) : size(imd.size), data(imd.data) {
	imd.data = nullptr;
}

template<typename T>
IndexedMetadata<T> &IndexedMetadata<T>::operator=(IndexedMetadata<T> &&imd) {
	delete [] data;
	size = imd.size;
	data = imd.data;
	imd.data = nullptr;
	return *this;
}



// Instantiate as required
template class DirectMetadata<std::string>;
template class DirectMetadata<double>;

template class MetadataIndex<std::string>;
template class MetadataIndex<double>;

template class IndexedMetadata<std::string>;
template class IndexedMetadata<double>;
