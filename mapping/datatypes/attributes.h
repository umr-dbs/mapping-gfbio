#ifndef RASTER_METADATA_H
#define RASTER_METADATA_H


#include <vector>
#include <map>
#include <string>
#include <sys/types.h>

class BinaryStream;

template<typename T>
class DirectMetadata {
	public:
		DirectMetadata();
		DirectMetadata(BinaryStream &stream);
		~DirectMetadata();

		void fromStream(BinaryStream &stream);
		void toStream(BinaryStream &stream) const;

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
class MetadataArrays {
	public:
		MetadataArrays();
		MetadataArrays(BinaryStream &stream);
		~MetadataArrays();

		void fromStream(BinaryStream &stream);
		void toStream(BinaryStream &stream) const;

		void set(size_t idx, const std::string &key, const T &value);
		const T &get(size_t idx, const std::string &key) const;
		const T &get(size_t idx, const std::string &key, const T &defaultvalue) const;

		std::vector<T> &addVector(const std::string &key, size_t capacity = 0);
		std::vector<T> &getVector(const std::string &key);
		const std::vector<T> &getVector(const std::string &key) const;

		//typename std::map<std::string, std::vector<T>>::iterator begin() { return data.begin(); }
		typename std::map<std::string, std::vector<T>>::const_iterator begin() const { return data.begin(); }

	    //typename std::map<std::string, std::vector<T>>::iterator end() { return data.end(); }
		typename std::map<std::string, std::vector<T>>::const_iterator end() const { return data.end(); }

		size_t size() const { return data.size(); }

		std::vector<std::string> getKeys() const;
	private:
		std::map<std::string, std::vector<T> > data;
};


#endif
