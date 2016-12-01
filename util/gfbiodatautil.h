#ifndef UTIL_GFBIODATAUTIL_H_
#define UTIL_GFBIODATAUTIL_H_

#include "util/make_unique.h"
#include "datatypes/spatiotemporal.h"

#include <pqxx/pqxx>


class GFBioDataUtil {
public:

	static std::string resolveTaxa(pqxx::connection &connection, std::string &scientificName);
	static std::string resolveTaxaNames(pqxx::connection &connection, std::string &scientificName);

	static size_t countGBIFResults(std::string &scientificName);

	static size_t countIUCNResults(std::string &scientificName);
};


#endif
