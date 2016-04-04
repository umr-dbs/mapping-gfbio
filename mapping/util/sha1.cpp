
#include "util/sha1.h"
#include "util/make_unique.h"

#include <sstream>

#include <boost/uuid/sha1.hpp>



SHA1::SHA1Value::SHA1Value(char *_value) {
	for (int i=0;i<20;i++)
		value[i] = _value[i];
}
SHA1::SHA1Value::~SHA1Value() {
}

std::string SHA1::SHA1Value::asHex() {
	std::ostringstream out;
	out << std::hex;
	for (int i=0;i<20;i++)
        out << ((value[i] & 0xf0) >> 4) << (value[i] & 0x0f);

	return out.str();
}



SHA1::SHA1() {
	s = make_unique<boost::uuids::detail::sha1>();
}

SHA1::~SHA1() {
}

void SHA1::addBytes(const char *data, size_t size) {
	s->process_bytes(data, size);
}

SHA1::SHA1Value SHA1::digest() {
	unsigned int digest[5];
	s->get_digest(digest);

	char digest_bytes[20];
	for (int i=0;i<5;i++) {
		const char *bytes = reinterpret_cast<const char*>(&digest[i]);
		digest_bytes[i*4+0] = bytes[3];
		digest_bytes[i*4+1] = bytes[2];
		digest_bytes[i*4+2] = bytes[1];
		digest_bytes[i*4+3] = bytes[0];
	}
	return SHA1Value(digest_bytes);
}

