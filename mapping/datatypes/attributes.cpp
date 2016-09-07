
#include "util/exceptions.h"
#include "datatypes/attributes.h"
#include "util/binarystream.h"

#include <limits>


AttributeMaps::AttributeMaps() {
}
AttributeMaps::~AttributeMaps() {
}

AttributeMaps::AttributeMaps(BinaryReadBuffer &buffer) {
	deserialize(buffer);
}

void AttributeMaps::deserialize(BinaryReadBuffer &buffer) {
	_numeric.empty();
	_textual.empty();
	auto count = buffer.read<size_t>();
	for (size_t i=0;i<count;i++) {
		auto key = buffer.read<std::string>();
		auto value = buffer.read<double>();
		_numeric[key] = value;
	}
	buffer.read(&count);
	for (size_t i=0;i<count;i++) {
		auto key = buffer.read<std::string>();
		auto value = buffer.read<std::string>();
		_textual[key] = value;
	}
}

void AttributeMaps::serialize(BinaryWriteBuffer &buffer, bool) const {
	size_t count = _numeric.size();
	buffer.write(count);
	for (auto &e : _numeric) {
		buffer.write(e.first);
		buffer.write(e.second);
	}
	count = _textual.size();
	buffer.write(count);
	for (auto &e : _textual) {
		buffer.write(e.first);
		buffer.write(e.second);
	}
}

void AttributeMaps::setNumeric(const std::string &key, double value) {
	if (_numeric.count(key) > 0)
		throw AttributeException(concat("Cannot set numeric attribute ", key, " because it's already set."));
	if (_textual.count(key) > 0)
		throw AttributeException(concat("Cannot set numeric attribute ", key, " because a textual attribute with the same name exists"));
	_numeric[key] = value;
}

void AttributeMaps::setTextual(const std::string &key, const std::string &value) {
	if (_textual.count(key) > 0)
		throw AttributeException(concat("Cannot set textual attribute ", key, " because it's already set."));
	if (_numeric.count(key) > 0)
		throw AttributeException(concat("Cannot set textual attribute ", key, " because a numeric attribute with the same name exists"));
	_textual[key] = value;
}

double AttributeMaps::getNumeric(const std::string &key) const {
	auto it = _numeric.find(key);
	if (it == _numeric.end())
		throw AttributeException(concat("Cannot get numeric attribute ", key, " because it does not exist"));
	return it->second;
}

const std::string &AttributeMaps::getTextual(const std::string &key) const {
	auto it = _textual.find(key);
	if (it == _textual.end())
		throw AttributeException(concat("Cannot get textual attribute ", key, " because it does not exist"));
	return it->second;
}

double AttributeMaps::getNumeric(const std::string &key, double defaultvalue) const {
	auto it = _numeric.find(key);
	if (it == _numeric.end()) {
		if (_textual.count("key"))
			throw AttributeException(concat("Cannot get numeric attribute ", key, " when a textual attribute with the same name exists"));
		return defaultvalue;
	}
	return it->second;
}

const std::string &AttributeMaps::getTextual(const std::string &key, const std::string &defaultvalue) const {
	auto it = _textual.find(key);
	if (it == _textual.end()) {
		if (_numeric.count("key"))
			throw AttributeException(concat("Cannot get textual attribute ", key, " when a numeric attribute with the same name exists"));
		return defaultvalue;
	}
	return it->second;
}



/**
 * AttributeArrays
 *
 * for SimpleFeatureCollections
 */
template <typename T>
void AttributeArrays::AttributeArray<T>::set(size_t idx, const T &value) {
	if (idx == array.size()) {
		array.push_back(value);
		return;
	}
	if (array.size() < idx+1)
		resize(idx+1);
	array[idx] = value;
}

template <typename T>
AttributeArrays::AttributeArray<T>::AttributeArray(BinaryReadBuffer &buffer) : unit(Unit::UNINITIALIZED) {
	deserialize(buffer);
}

template <typename T>
void AttributeArrays::AttributeArray<T>::deserialize(BinaryReadBuffer &buffer) {
	auto unit_json = buffer.read<std::string>();
	unit = Unit(unit_json);
	buffer.read(&array);
}

template <typename T>
void AttributeArrays::AttributeArray<T>::serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const {
	buffer << unit.toJson();
	buffer.write(array, is_persistent_memory);
}

template<typename T> struct defaultvalue {
	static const T value;
};
template<>
const double defaultvalue<double>::value = std::numeric_limits<double>::quiet_NaN();
template<>
const std::string defaultvalue<std::string>::value = "";

template <typename T>
void AttributeArrays::AttributeArray<T>::resize(size_t size) {
	array.resize(size, defaultvalue<T>::value);
}

template<typename T>
size_t AttributeArrays::AttributeArray<T>::get_byte_size() const {
	return unit.get_byte_size() + SizeUtil::get_byte_size(array);
}

template<typename T>
AttributeArrays::AttributeArray<T> AttributeArrays::AttributeArray<T>::copy() const {
	AttributeArray<T> res(unit);
	res.array = array;
	return res;
}





AttributeArrays::AttributeArrays() {
}
AttributeArrays::AttributeArrays(BinaryReadBuffer &buffer) {
	deserialize(buffer);
}

AttributeArrays::~AttributeArrays() {
}

void AttributeArrays::deserialize(BinaryReadBuffer &buffer) {
	auto keycount = buffer.read<size_t>();
	for (size_t i=0;i<keycount;i++) {
		auto key = buffer.read<std::string>();
		auto res = _numeric.emplace(key, buffer);
		if (res.second != true)
			throw AttributeException("Cannot deserialize AttributeArrays");
	}

	buffer.read(&keycount);
	for (size_t i=0;i<keycount;i++) {
		auto key = buffer.read<std::string>();
		auto res = _textual.emplace(key, buffer);
		if (res.second != true)
			throw AttributeException("Cannot deserialize AttributeArrays");
	}
}

void AttributeArrays::serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const {
	size_t keycount = _numeric.size();
	buffer << keycount;
	for (const auto &e : _numeric) {
		buffer << e.first << e.second;
	}

	keycount = _textual.size();
	buffer << keycount;
	for (const auto &e : _textual) {
		buffer << e.first << e.second;
	}
}

AttributeArrays AttributeArrays::clone() const {
	AttributeArrays copy;
	// The Array's copy constructor is neither callable from std::pair nor std::map.
	// Thus, we need to make sure the copying takes place right here, because we're friends.
	for (auto &pair : _numeric) {
		auto arraycopy = pair.second;
		copy._numeric.emplace(pair.first, std::move(arraycopy));
	}
	for (auto &pair : _textual) {
		auto arraycopy = pair.second;
		copy._textual.emplace(pair.first, std::move(arraycopy));
	}
	return copy;
}

void AttributeArrays::checkIfAttributeDoesNotExist(const std::string &key) {
	if (_numeric.count(key) > 0)
		throw AttributeException(concat("Cannot add attribute ", key, " because a numeric attribute with the same name exists."));
	if (_textual.count(key) > 0)
		throw AttributeException(concat("Cannot add attribute ", key, " because a textual attribute with the same name exists."));
}

AttributeArrays::AttributeArray<double> &AttributeArrays::addNumericAttribute(const std::string &key, const Unit &unit) {
	checkIfAttributeDoesNotExist(key);

	auto res = _numeric.emplace(key, unit);
	if (res.second != true)
		throw AttributeException(concat("Cannot add numeric attribute ", key, " because it exists already."));

	return (res.first)->second;
}

AttributeArrays::AttributeArray<double> &AttributeArrays::addNumericAttribute(const std::string &key, const Unit &unit, std::vector<double> &&values) {
	checkIfAttributeDoesNotExist(key);

	auto res = _numeric.emplace(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple(unit, std::move(values)));
	if (res.second != true)
		throw AttributeException(concat("Cannot add numeric attribute ", key, " because it exists already."));

	return (res.first)->second;
}

AttributeArrays::AttributeArray<std::string> &AttributeArrays::addTextualAttribute(const std::string &key, const Unit &unit) {
	checkIfAttributeDoesNotExist(key);

	auto res = _textual.emplace(key, unit);
	if (res.second != true)
		throw AttributeException(concat("Cannot add textual attribute ", key, " because it exists already."));

	return (res.first)->second;
}

AttributeArrays::AttributeArray<std::string> &AttributeArrays::addTextualAttribute(const std::string &key, const Unit &unit, std::vector<std::string> &&values) {
	checkIfAttributeDoesNotExist(key);

	auto res = _textual.emplace(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple(unit, std::move(values)));
	if (res.second != true)
		throw AttributeException(concat("Cannot add textual attribute ", key, " because it exists already."));

	return (res.first)->second;
}


std::vector<std::string> AttributeArrays::getNumericKeys() const {
	std::vector<std::string> keys;
	for (auto &p : _numeric) {
		keys.push_back(p.first);
	}
	return keys;
}
std::vector<std::string> AttributeArrays::getTextualKeys() const {
	std::vector<std::string> keys;
	for (auto &p : _textual) {
		keys.push_back(p.first);
	}
	return keys;
}

template<typename T>
AttributeArrays AttributeArrays::filter_impl(const std::vector<T> &keep, size_t kept_count) const {
	// If the kept_count wasn't provided, start counting
	if (kept_count == 0) {
		for (auto b : keep) {
			if (b)
				kept_count++;
		}
	}

	AttributeArrays out;

	for (auto &p : _numeric) {
		const auto &in_array = p.second;
		if (in_array.array.size() != keep.size())
			throw AttributeException("Cannot filter Attributes when the keep vector has a different size than the attribute vectors");
		auto &out_array = out.addNumericAttribute(p.first, in_array.unit);
		out_array.array.reserve(kept_count);

		for (size_t in_idx = 0; in_idx < keep.size(); in_idx++) {
			if (keep[in_idx])
				out_array.array.push_back(in_array.array[in_idx]);
		}
	}
	for (auto &p : _textual) {
		const auto &in_array = p.second;
		if (in_array.array.size() != keep.size())
			throw AttributeException("Cannot filter Attributes when the keep vector has a different size than the attribute vectors");
		auto &out_array = out.addTextualAttribute(p.first, in_array.unit);
		out_array.array.reserve(kept_count);

		for (size_t in_idx = 0; in_idx < keep.size(); in_idx++) {
			if (keep[in_idx])
				out_array.array.push_back(in_array.array[in_idx]);
		}
	}

	return out;
}

AttributeArrays AttributeArrays::filter(const std::vector<bool> &keep, size_t kept_count) const {
	return filter_impl<bool>(keep, kept_count);
}
AttributeArrays AttributeArrays::filter(const std::vector<char> &keep, size_t kept_count) const {
	return filter_impl<char>(keep, kept_count);
}

void AttributeArrays::validate(size_t expected_values) const {
	for (auto &n : _numeric) {
		if (n.second.array.size() != expected_values)
			throw AttributeException(concat("Numeric attribute array ", n.first, " does not contain the expected amount of values (expected: ", expected_values, " actual: ", n.second.array.size(), ")"));
	}
	for (auto &n : _textual) {
		if (n.second.array.size() != expected_values)
			throw AttributeException(concat("Textual attribute array ", n.first, " does not contain the expected amount of values (expected: ", expected_values, " actual: ", n.second.array.size(), ")"));
	}
}

size_t AttributeArrays::get_byte_size() const {
	return SizeUtil::get_byte_size(_textual) + SizeUtil::get_byte_size(_numeric);
}


AttributeArrays AttributeArrays::copy() const {
	AttributeArrays res;
	for ( auto &e : _numeric )
		res._numeric.emplace(e.first,e.second.copy());
	for ( auto &e : _textual )
		res._textual.emplace(e.first,e.second.copy());
	return res;
}

// Instantiate as required
template class AttributeArrays::AttributeArray<double>;
template class AttributeArrays::AttributeArray<std::string>;

void AttributeArrays::resize(size_t size) {
	for(std::string key : getTextualKeys()) {
		textual(key).resize(size);
	}
	for(std::string key : getNumericKeys()) {
		numeric(key).resize(size);
	}
}

void AttributeArrays::renameNumericAttribute(const std::string &oldKey, const std::string &newKey) {
	if (_numeric.count(oldKey) < 1)
		throw ArgumentException("AttributeArray::rename oldKey does not exist");

	if (_numeric.count(newKey) > 0)
		throw ArgumentException("AttributeArray::rename newKey already exist");

	AttributeArray<double> &temp = _numeric.at(oldKey);
	_numeric.insert(std::make_pair(newKey, std::move(temp)));
	_numeric.erase(oldKey);
}

void AttributeArrays::renameTextualAttribute(const std::string &oldKey, const std::string &newKey) {
	if (_textual.count(oldKey) < 1)
		throw ArgumentException("AttributeArray::rename oldKey does not exist");

	if (_textual.count(newKey) > 0)
		throw ArgumentException("AttributeArray::rename newKey already exist");

	AttributeArray<std::string> &temp = _textual.at(oldKey);
	_textual.insert(std::make_pair(newKey, std::move(temp)));
	_textual.erase(oldKey);
}
