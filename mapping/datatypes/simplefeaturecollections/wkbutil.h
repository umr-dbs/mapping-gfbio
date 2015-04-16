#ifndef UTIL_WKBPARSER_H_
#define UTIL_WKBPARSER_H_

#include "datatypes/multipolygoncollection.h"

/**
 * Utility class to read/write WKB to/from collection instances
 */
class WKBUtil {
public:
	WKBUtil() = delete;

	static std::unique_ptr<MultiPolygonCollection> readMultiPolygonCollection(std::stringstream& wkb);

};

#endif
