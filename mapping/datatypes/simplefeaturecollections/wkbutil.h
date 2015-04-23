#ifndef UTIL_WKBPARSER_H_
#define UTIL_WKBPARSER_H_

#include "datatypes/polygoncollection.h"
#include <memory>

/**
 * Utility class to read/write WKB to/from collection instances
 */
class WKBUtil {
public:
	WKBUtil() = delete;

	static std::unique_ptr<PolygonCollection> readPolygonCollection(std::stringstream& wkb);
	static std::unique_ptr<PolygonCollection> readPolygonCollection(std::string& wkt);
};

#endif
