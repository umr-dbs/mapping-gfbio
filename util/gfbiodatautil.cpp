
#include "util/make_unique.h"
#include "util/exceptions.h"
#include "util/enumconverter.h"
#include "gfbiodatautil.h"
#include "util/configuration.h"


std::string GFBioDataUtil::resolveTaxa(pqxx::connection &connection, std::string &scientificName) {
	connection.prepare("taxa", "SELECT DISTINCT taxon FROM gbif.gbif_taxon_to_name WHERE name ILIKE $1");
	pqxx::work work(connection);
	pqxx::result result = work.prepared("taxa")(scientificName + "%").exec();

	std::stringstream taxa;
	taxa << "{";
	for(size_t i = 0; i < result.size(); ++i) {
		if(i != 0)
			taxa << ",";
		taxa << result[i][0];
	}
	taxa << "}";
	return taxa.str();
}

std::string GFBioDataUtil::resolveTaxaNames(pqxx::connection &connection, std::string &scientificName) {
	connection.prepare("taxa", "SELECT DISTINCT lower(name) FROM gbif.gbif_taxon_to_name WHERE name ILIKE $1");
	pqxx::work work(connection);
	pqxx::result result = work.prepared("taxa")(scientificName + "%").exec();

	std::stringstream taxa;
	taxa << "{";
	for(size_t i = 0; i < result.size(); ++i) {
		if(i != 0)
			taxa << ",";
		taxa << result[i][0];
	}
	taxa << "}";
	return taxa.str();
}

size_t GFBioDataUtil::countGBIFResults(std::string &scientificName) {
	pqxx::connection connection (Configuration::get("operators.gbifsource.dbcredentials"));

	std::string taxa = resolveTaxa(connection, scientificName);

	connection.prepare("occurrences", "SELECT count(*) FROM gbif.gbif_lite_time WHERE taxon = ANY($1)");

	pqxx::work work(connection);
	pqxx::result result = work.prepared("occurrences")(taxa).exec();
	work.commit();

	return result[0][0].as<size_t>();
}

size_t GFBioDataUtil::countIUCNResults(std::string &scientificName) {
	pqxx::connection connection (Configuration::get("operators.gbifsource.dbcredentials"));

	std::string taxa = resolveTaxaNames(connection, scientificName);
	connection.prepare("occurrences", "SELECT count(*) FROM iucn.expert_ranges_all WHERE lower(binomial) = ANY($1)");

	pqxx::work work(connection);
	pqxx::result result = work.prepared("occurrences")(taxa).exec();
	work.commit();

	return result[0][0].as<size_t>();
}
