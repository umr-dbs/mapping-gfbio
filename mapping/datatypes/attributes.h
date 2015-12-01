#ifndef RASTER_METADATA_H
#define RASTER_METADATA_H

#include "datatypes/unit.h"
#include "util/sizeutil.h"

#include <vector>
#include <map>
#include <string>
#include <sys/types.h>

class BinaryStream;

namespace RasterOpenCL {
	class CLProgram;
}


/**
 * @class AttributeMaps
 *
 * Contains a set of key/value pairs, where the values can be either
 * numeric (stored as double) or textual (stored as std::string).
 */
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
		/**
		 * Default constructor. No key/value pairs are set.
		 */
		AttributeMaps();
		/**
		 * Construct a new object from a stream by deserialization.
		 * @param stream a binary stream
		 */
		AttributeMaps(BinaryStream &stream);
		/**
		 * Destructor.
		 */
		~AttributeMaps();

		/**
		 * Deserialize the object state from a stream
		 * @param stream a binary stream
		 */
		void fromStream(BinaryStream &stream);
		/**
		 * Serialize the object state to a stream
		 * @param stream a binary stream
		 */
		void toStream(BinaryStream &stream) const;

		/**
		 * Set a numeric key/value pair. Throws an exception if the key is already set.
		 * @param key the key
		 * @param value the value
		 */
		void setNumeric(const std::string &key, double value);
		/**
		 * Set a textual key/value pair. Throws an exception if the key is already set.
		 * @param key the key
		 * @param value the value
		 */
		void setTextual(const std::string &key, const std::string &value);

		/**
		 * Gets a numeric value matching the given key. Throws an exception if the key is either not set or not numeric.
		 * @param key the key
		 * @return the value
		 */
		double getNumeric(const std::string &key) const;
		/**
		 * Gets a numeric value matching the given key. Returns the default value if the key is not set.
		 * Throws an exception if the key is set, but not numeric.
		 * @param key the key
		 * @param defaultvalue the default value
		 * @return the value
		 */
		double getNumeric(const std::string &key, double defaultvalue) const;
		/**
		 * Gets a textual value matching the given key. Throws an exception if the key is either not set or not textual.
		 * @param key the key
		 * @return the value
		 */
		const std::string &getTextual(const std::string &key) const;
		/**
		 * Gets a textual value matching the given key. Returns the default value if the key is not set.
		 * Throws an exception if the key is set, but not textual.
		 * @param key the key
		 * @param defaultvalue the default value
		 * @return the value
		 */
		const std::string &getTextual(const std::string &key, const std::string &defaultvalue) const;

		/**
		 * Returns an iterable object allowing read-only iteration over all numeric key/value-pairs
		 * @return the iterable object
		 */
		ConstIterableMapReference<double> numeric() const { return ConstIterableMapReference<double>(_numeric); }
		/**
		 * Returns an iterable object allowing read-only iteration over all textual key/value-pairs
		 * @return the iterable object
		 */
		ConstIterableMapReference<std::string> textual() const { return ConstIterableMapReference<std::string>(_textual); }

		/**
		 * the size of this object in memory (in bytes)
		 * @return the size of this object in bytes
		 */
		size_t get_byte_size() const { return SizeUtil::get_byte_size(_numeric) + SizeUtil::get_byte_size(_textual ); }

	private:
		std::map<std::string, double> _numeric;
		std::map<std::string, std::string> _textual;
};


/**
 * @class AttributeArrays
 *
 * Contains a set of key/value pairs, where each value is actually an array of values.
 * Use this class to store homogeneous attributes for multiple object, e.g. one attribute value for
 * each feature in a SimpleFeatureCollection.
 *
 * Like AttributeMaps, values are either numeric (stored as double) or textual (stored as std::string).
 */
class AttributeArrays {
	private:
		template <typename T>
		class AttributeArray {
			public:
				using array_type = std::vector<T>;

				AttributeArray(const Unit &unit) : unit(unit) {}
				AttributeArray(const Unit &unit, std::vector<T> &&values) : unit(unit), array(values) {}
				// prevent accidental copies
			private:
				AttributeArray(const AttributeArray &) = default;
				AttributeArray& operator=(const AttributeArray &) = default;
			public:
				// moves are OK
				AttributeArray(AttributeArray &&) = default;
				AttributeArray& operator=(AttributeArray &&) = default;

				AttributeArray(BinaryStream &stream);
				void fromStream(BinaryStream &stream);
				void toStream(BinaryStream &stream) const;

				/**
				 * Sets an attribute value.
				 *
				 * @param idx index of the attribute to set
				 * @param value value to set
				 */
				void set(size_t idx, const T &value);
				/**
				 * Gets an attribute value
				 *
				 * @param idx index of the attribute to read
				 * @return value of the attribute
				 */
				const T &get(size_t idx) const {
					return array.at(idx);
				}

				/**
				 * Reserves memory in the underlying array
				 *
				 * @param size desired size
				 */
				void reserve(size_t size) { array.reserve(size); }

				/**
				 * Resizes the underlying array
				 *
				 * @param size desired size
				 */
				void resize(size_t size);

				/**
				 * the size of this object in memory (in bytes)
				 * @return the size of this object in bytes
				 */
				size_t get_byte_size() const;

				Unit unit;
			private:
				AttributeArray<T> copy() const;

				// to map the array to a GPU, direct access to the std::vector is required
				friend class RasterOpenCL::CLProgram;
				friend AttributeArrays;
				friend class AttributeArraysHelper;

				array_type array;
		};
	public:
		AttributeArrays();
		AttributeArrays(BinaryStream &stream);
		~AttributeArrays();

		// explicitely mark this class as movable, but not copyable.
		AttributeArrays(const AttributeArrays &) = delete;
		AttributeArrays& operator=(const AttributeArrays &) = delete;
		AttributeArrays(AttributeArrays &&) = default;
		AttributeArrays& operator=(AttributeArrays &&) = default;

		// if you must have a copy, create one explicitely via clone
		AttributeArrays clone() const;

		void fromStream(BinaryStream &stream);
		void toStream(BinaryStream &stream) const;

		/**
		 * Returns a reference to a numeric attribute array
		 *
		 * @param key the attribute name
		 * @return a reference to the array
		 */
		AttributeArray<double> &numeric(const std::string &key) { return _numeric.at(key); }
		/**
		 * Returns a reference to a textual attribute array
		 *
		 * @param key the attribute name
		 * @return a reference to the array
		 */
		AttributeArray<std::string> &textual(const std::string &key) { return _textual.at(key); }

		/**
		 * Returns a const reference to a numeric attribute array
		 *
		 * @param key the attribute name
		 * @return a const reference to the array
		 */
		const AttributeArray<double> &numeric(const std::string &key) const { return _numeric.at(key); }
		/**
		 * Returns a const reference to a textual attribute array
		 *
		 * @param key the attribute name
		 * @return a const reference to the array
		 */
		const AttributeArray<std::string> &textual(const std::string &key) const { return _textual.at(key); }

		/**
		 * Adds a new numeric attribute. The new attribute array is "empty" and has no values set.
		 *
		 * @param key the attribute name
		 * @param unit the unit of the attribute values
		 *
		 * @return a reference to the new array
		 */
		AttributeArray<double> &addNumericAttribute(const std::string &key, const Unit &unit);
		/**
		 * Adds a new numeric attribute from a vector of values
		 *
		 * @param key the attribute name
		 * @param unit the unit of the attribute values
		 * @param values a movable vector of attribute values
		 *
		 * @return a reference to the new array
		 */
		AttributeArray<double> &addNumericAttribute(const std::string &key, const Unit &unit, std::vector<double> &&values);
		/**
		 * Adds a new textual attribute. The new attribute array is "empty" and has no values set.
		 *
		 * @param key the attribute name
		 * @param unit the unit of the attribute values
		 *
		 * @return a reference to the new array
		 */
		AttributeArray<std::string> &addTextualAttribute(const std::string &key, const Unit &unit);
		/**
		 * Adds a new textual attribute from a vector of values
		 *
		 * @param key the attribute name
		 * @param unit the unit of the attribute values
		 * @param values a movable vector of attribute values
		 *
		 * @return a reference to the new array
		 */
		AttributeArray<std::string> &addTextualAttribute(const std::string &key, const Unit &unit, std::vector<std::string> &&values);

		/**
		 * Returns an iterable collection containing the names of all numeric attributes
		 *
		 * @return an iterable collection containing the names of all numeric attributes
		 */
		std::vector<std::string> getNumericKeys() const;
		/**
		 * Returns an iterable collection containing the names of all textual attributes
		 *
		 * @return an iterable collection containing the names of all textual attributes
		 */
		std::vector<std::string> getTextualKeys() const;

		/**
		 * Creates a new AttributeArrays object, filtered by index.
		 *
		 * @param keep a vector with the same length as the attribute arrays, containing true for all values to keep
		 * @param kept_count (optional) the count of all true keep-values
		 *
		 * @return a new AttributeArrays object, containing the same attributes, but filtered by values.
		 */
		AttributeArrays filter(const std::vector<bool> &keep, size_t kept_count = 0) const;
		AttributeArrays filter(const std::vector<char> &keep, size_t kept_count = 0) const;
	private:
		template<typename T>
			AttributeArrays filter_impl(const std::vector<T> &keep, size_t kept_count) const;
	public:

		/**
		 * Validates the invariants. All attribute arrays need to be the same size.
		 *
		 * Throws an exception when invariants are violated.
		 */
		void validate(size_t expected_values) const;


		/**
		 * the size of this object in memory (in bytes)
		 * @return the size of this object in bytes
		 */
		size_t get_byte_size() const;

		AttributeArrays copy() const;

	private:
		void checkIfAttributeDoesNotExist(const std::string &key);

		std::map<std::string, AttributeArray<double> > _numeric;
		std::map<std::string, AttributeArray<std::string> > _textual;
		friend class AttributeArraysHelper;
};

#endif
