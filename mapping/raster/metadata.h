#ifndef RASTER_METADATA_H
#define RASTER_METADATA_H


#include <vector>
#include <map>
#include <string>
#include <sys/types.h>


template<typename T>
class DirectMetadata {
	public:
		DirectMetadata();
		~DirectMetadata();
		void set(const std::string &key, const T &value);
		const T &get(const std::string &key) const;
		const T &get(const std::string &key, const T &defaultvalue) const;

		//typename std::map<std::string, T>::iterator begin() { return data.begin(); }
		typename std::map<std::string, T>::const_iterator begin() const { return data.begin(); }

	    //typename std::map<std::string, T>::iterator end() { return data.end(); }
		typename std::map<std::string, T>::const_iterator end() const { return data.end(); }
	private:
		std::map<std::string, T> data;
};

template<typename T>
class IndexedMetadata;

template<typename T>
class MetadataIndex {
	public:
		typedef uint8_t metadata_index_t;
		MetadataIndex();
		~MetadataIndex();
		void addKey(const std::string &key);
		void lock();
		void setValue(IndexedMetadata<T> &object, const std::string &key, const T &value);
		const T &getValue(const IndexedMetadata<T> &object, const std::string &key) const;
		metadata_index_t size() { return index.size(); }

		typename std::map<std::string, metadata_index_t>::const_iterator begin() const { return index.begin(); }
		typename std::map<std::string, metadata_index_t>::const_iterator end() const { return index.end(); }
	private:
		std::map<std::string, metadata_index_t> index;
		bool index_is_locked;
};


template<typename T>
class IndexedMetadata {
	public:
		typedef typename MetadataIndex<T>::metadata_index_t metadata_index_t;
		IndexedMetadata(metadata_index_t count);
		//IndexedMetadata(const MetadataIndex &index);
		~IndexedMetadata();

		// Copy
		IndexedMetadata(const IndexedMetadata<T> &imd);
		IndexedMetadata<T> &operator=(const IndexedMetadata<T> &imd);
		// Move
		IndexedMetadata(IndexedMetadata<T> &&imd);
		IndexedMetadata<T> &operator=(IndexedMetadata<T> &&imd);
	private:
		metadata_index_t size;
		T *data;

		friend class MetadataIndex<T>;
};


#endif
