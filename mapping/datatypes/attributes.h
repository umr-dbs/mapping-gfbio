#ifndef RASTER_METADATA_H
#define RASTER_METADATA_H


#include <vector>
#include <map>
#include <string>
#include <sys/types.h>

class BinaryStream;


template <typename T>
class ConstIterableMapReference {
	public:
		using map_type = typename std::map<std::string, T>;
		using iterator = typename map_type::const_iterator;

		ConstIterableMapReference(const map_type &map) : map(map) {};

		iterator begin() const noexcept { return map.cbegin(); }
		iterator end() const noexcept { return map.cend(); }

	private:
		const std::map<std::string, T> &map;
};


class AttributeMaps {
	public:
		AttributeMaps();
		AttributeMaps(BinaryStream &stream);
		~AttributeMaps();

		void fromStream(BinaryStream &stream);
		void toStream(BinaryStream &stream) const;

		void setNumeric(const std::string &key, double value);
		void setTextual(const std::string &key, const std::string &value);

		double getNumeric(const std::string &key) const;
		double getNumeric(const std::string &key, double defaultvalue) const;
		const std::string &getTextual(const std::string &key) const;
		const std::string &getTextual(const std::string &key, const std::string &defaultvalue) const;

		ConstIterableMapReference<double> numeric() const { return ConstIterableMapReference<double>(_numeric); }
		ConstIterableMapReference<std::string> textual() const { return ConstIterableMapReference<std::string>(_textual); }
	private:
		std::map<std::string, double> _numeric;
		std::map<std::string, std::string> _textual;
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

		std::vector<T> &addVector(const std::string &key, size_t capacity);
		std::vector<T> &addEmptyVector(const std::string &key, size_t reserve = 0);
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
