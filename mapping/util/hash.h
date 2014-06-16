#ifndef RASTER_HASH_H
#define RASTER_HASH_H

#include <string>
#include <stdint.h>


class HashResult {
	public:
		HashResult(uint64_t val1, uint64_t val2) : val1(val1), val2(val2) {};
		std::string asHex();
		const uint64_t val1, val2;
};

HashResult calculateHash(const uint8_t *data, const int len);

#endif
