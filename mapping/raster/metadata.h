#ifndef RASTER_METADATA_H
#define RASTER_METADATA_H


#include <vector>
#include <map>
#include <string>
#include <sys/types.h>

class Socket;

template<typename T>
class DirectMetadata {
	public:
		DirectMetadata();
		DirectMetadata(Socket &socket);
		~DirectMetadata();

		void fromSocket(Socket &socket);
		void toSocket(Socket &socket) const;

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
		MetadataArrays(Socket &socket);
		~MetadataArrays();

		void fromSocket(Socket &socket);
		void toSocket(Socket &socket) const;

		void set(size_t idx, const std::string &key, const T &value);
		const T &get(size_t idx, const std::string &key) const;
		const T &get(size_t idx, const std::string &key, const T &defaultvalue) const;

		std::vector<T> &addVector(const std::string &key, size_t capacity = 0);
		std::vector<T> &getVector(const std::string &key);

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
