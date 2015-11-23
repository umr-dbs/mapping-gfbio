#ifndef RASTER_METADATA_H
#define RASTER_METADATA_H

#include "datatypes/unit.h"

#include <vector>
#include <map>
#include <string>
#include <sys/types.h>

class BinaryStream;

namespace RasterOpenCL {
	class CLProgram;
}


class AttributeMaps {
	private:
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


class AttributeArrays {
	private:
		template <typename T>
		class AttributeArray {
			public:
				using array_type = std::vector<T>;

				AttributeArray(const Unit &unit) : unit(unit) {}
				AttributeArray(const Unit &unit, std::vector<T> &&values) : unit(unit), array(values) {}
				// prevent accidental copies
				AttributeArray(const AttributeArray &) = delete;
				AttributeArray& operator=(const AttributeArray &) = delete;
				// moves are OK
				AttributeArray(AttributeArray &&) = default;
				AttributeArray& operator=(AttributeArray &&) = default;

				AttributeArray(BinaryStream &stream);
				void fromStream(BinaryStream &stream);
				void toStream(BinaryStream &stream) const;

				void set(size_t idx, const T &value);
				const T &get(size_t idx) const {
					return array.at(idx);
				}

				void reserve(size_t size) { array.reserve(size); }
				void resize(size_t size);

				Unit unit;
			private:
				AttributeArray<T>& operator+=( const AttributeArray<T> &other );

				// to map the array to a GPU, direct access to the std::vector is required
				friend class RasterOpenCL::CLProgram;
				friend AttributeArrays;

				array_type array;
		};
	public:
		AttributeArrays();
		AttributeArrays(BinaryStream &stream);
		~AttributeArrays();

		// explicitely mark this class as movable, but not copyable. It should be exactly that,
		// because AttributeArray<T> is only movable, but the compiler doesn't seem to figure that out on its own.
		AttributeArrays(const AttributeArrays &) = delete;
		AttributeArrays& operator=(const AttributeArrays &) = delete;
		AttributeArrays(AttributeArrays &&) = default;
		AttributeArrays& operator=(AttributeArrays &&) = default;

		void fromStream(BinaryStream &stream);
		void toStream(BinaryStream &stream) const;

		AttributeArray<double> &numeric(const std::string &key) { return _numeric.at(key); }
		AttributeArray<std::string> &textual(const std::string &key) { return _textual.at(key); }

		const AttributeArray<double> &numeric(const std::string &key) const { return _numeric.at(key); }
		const AttributeArray<std::string> &textual(const std::string &key) const { return _textual.at(key); }

		AttributeArray<double> &addNumericAttribute(const std::string &key, const Unit &unit);
		AttributeArray<double> &addNumericAttribute(const std::string &key, const Unit &unit, std::vector<double> &&values);
		AttributeArray<std::string> &addTextualAttribute(const std::string &key, const Unit &unit);
		AttributeArray<std::string> &addTextualAttribute(const std::string &key, const Unit &unit, std::vector<std::string> &&values);

		std::vector<std::string> getNumericKeys() const;
		std::vector<std::string> getTextualKeys() const;

		AttributeArrays filter(const std::vector<bool> &keep, size_t kept_count = 0) const;
		AttributeArrays filter(const std::vector<char> &keep, size_t kept_count = 0) const;

		AttributeArrays& operator+=( const AttributeArrays &other );
	private:
		template<typename T>
			AttributeArrays filter_impl(const std::vector<T> &keep, size_t kept_count) const;
	public:

		void validate(size_t expected_values) const;

	private:
		void checkIfAttributeDoesNotExist(const std::string &key);

		std::map<std::string, AttributeArray<double> > _numeric;
		std::map<std::string, AttributeArray<std::string> > _textual;
};

#endif
