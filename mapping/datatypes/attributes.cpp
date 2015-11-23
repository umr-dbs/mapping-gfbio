
#include "util/exceptions.h"
#include "datatypes/attributes.h"
#include "util/binarystream.h"

#include <limits>


AttributeMaps::AttributeMaps() {
}
AttributeMaps::~AttributeMaps() {
}

AttributeMaps::AttributeMaps(BinaryStream &stream) {
	fromStream(stream);
}

void AttributeMaps::fromStream(BinaryStream &stream) {
	_numeric.empty();
	_textual.empty();
	size_t count;
	stream.read(&count);
	for (size_t i=0;i<count;i++) {
		std::string key;
		stream.read(&key);
		double value;
		stream.read(&value);
		_numeric[key] = value;
	}
	stream.read(&count);
	for (size_t i=0;i<count;i++) {
		std::string key;
		stream.read(&key);
		std::string value;
		stream.read(&value);
		_textual[key] = value;
	}
}

void AttributeMaps::toStream(BinaryStream &stream) const {
	size_t count = _numeric.size();
	stream.write(count);
	for (auto &e : _numeric) {
		stream.write(e.first);
		stream.write(e.second);
	}
	count = _textual.size();
	stream.write(count);
	for (auto &e : _textual) {
		stream.write(e.first);
		stream.write(e.second);
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
	if (it == _numeric.end())
		return defaultvalue;
	return it->second;
}

const std::string &AttributeMaps::getTextual(const std::string &key, const std::string &defaultvalue) const {
	auto it = _textual.find(key);
	if (it == _textual.end())
		return defaultvalue;
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
		array.resize(idx+1);
	array[idx] = value;
}

template <typename T>
AttributeArrays::AttributeArray<T>::AttributeArray(BinaryStream &stream) : unit(Unit::UNINITIALIZED) {
	fromStream(stream);
}

template <typename T>
void AttributeArrays::AttributeArray<T>::fromStream(BinaryStream &stream) {
	std::string unit_json;
	stream.read(&unit_json);
	unit = Unit(unit_json);
	size_t size;
	stream.read(&size);
	array.reserve(size);
	for (size_t i=0;i<size;i++) {
		T value;
		stream.read(&value);
		array.push_back(value);
	}

}

template <typename T>
void AttributeArrays::AttributeArray<T>::toStream(BinaryStream &stream) const {
	std::string unit_json = unit.toJson();
	stream.write(unit_json);
	stream.write(array.size());
	for (const auto &v : array)
		stream.write(&v);
}

template<typename T>
AttributeArrays::AttributeArray<T>& AttributeArrays::AttributeArray<T>::operator +=(
		const AttributeArray<T>& other) {

	reserve(array.size() + other.array.size() );
	array.insert(array.end(), other.array.begin(), other.array.end() );
	return *this;
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


AttributeArrays::AttributeArrays() {
}
AttributeArrays::AttributeArrays(BinaryStream &stream) {
	fromStream(stream);
}

AttributeArrays::~AttributeArrays() {
}

void AttributeArrays::fromStream(BinaryStream &stream) {
	size_t keycount;
	stream.read(&keycount);
	for (size_t i=0;i<keycount;i++) {
		std::string key;
		stream.read(&key);
		auto res = _numeric.emplace(key, stream);
		if (res.second != true)
			throw AttributeException("Cannot deserialize AttributeArrays");
	}

	stream.read(&keycount);
	for (size_t i=0;i<keycount;i++) {
		std::string key;
		stream.read(&key);
		auto res = _textual.emplace(key, stream);
		if (res.second != true)
			throw AttributeException("Cannot deserialize AttributeArrays");
	}
}

void AttributeArrays::toStream(BinaryStream &stream) const {
	size_t keycount = _numeric.size();
	stream.write(keycount);
	for (const auto &e : _numeric) {
		const auto &key = e.first;
		stream.write(key);
		const auto &vec = e.second;
		stream.write(vec);
	}

	keycount = _textual.size();
	stream.write(keycount);
	for (const auto &e : _textual) {
		const auto &key = e.first;
		stream.write(key);
		const auto &vec = e.second;
		stream.write(vec);
	}
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
			throw AttributeException(concat("Numeric attribute array ", n.first, " does not contain the expected amount of values"));
	}
	for (auto &n : _textual) {
		if (n.second.array.size() != expected_values)
			throw AttributeException(concat("Numeric attribute array ", n.first, " does not contain the expected amount of values"));
	}
}

AttributeArrays& AttributeArrays::operator +=(const AttributeArrays& other) {
	for (auto &n : _numeric) {
		n.second += other._numeric.at(n.first);
	}
	for (auto &n : _textual) {
		n.second += other._textual.at(n.first);
	}
	return *this;
}


// Instantiate as required
template class AttributeArrays::AttributeArray<double>;
template class AttributeArrays::AttributeArray<std::string>;


