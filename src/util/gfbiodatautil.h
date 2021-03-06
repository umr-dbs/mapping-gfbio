#ifndef UTIL_GFBIODATAUTIL_H_
#define UTIL_GFBIODATAUTIL_H_

#include "datatypes/spatiotemporal.h"

#include <pqxx/pqxx>


class GFBioDataUtil {
public:

	static std::string resolveTaxa(pqxx::connection &connection, std::string &term, std::string &level);
	static std::string resolveTaxaNames(pqxx::connection &connection, std::string &term, std::string &level);

	static size_t countGBIFResults(std::string &term, std::string &level);

	static size_t countIUCNResults(std::string &term, std::string &level);

	static std::vector<std::string> getAvailableABCDArchives();

	static Json::Value getGFBioDataCentersJSON();

};


#endif
