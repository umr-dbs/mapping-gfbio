
#include "util/make_unique.h"
#include "util/exceptions.h"
#include "util/enumconverter.h"
#include "gfbiodatautil.h"
#include "util/configuration.h"

#include <fstream>


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
	pqxx::connection connection (Configuration::get<std::string>("operators.gfbiosource.dbcredentials"));

	std::string taxa = resolveTaxa(connection, scientificName);

	connection.prepare("occurrences", "SELECT count(*) FROM gbif.gbif_lite_time WHERE taxon = ANY($1) AND geom IS NOT NULL");

	pqxx::work work(connection);
	pqxx::result result = work.prepared("occurrences")(taxa).exec();
	work.commit();

	return result[0][0].as<size_t>();
}

size_t GFBioDataUtil::countIUCNResults(std::string &scientificName) {
	pqxx::connection connection (Configuration::get<std::string>("operators.gfbiosource.dbcredentials"));

	std::string taxa = resolveTaxaNames(connection, scientificName);
	connection.prepare("occurrences", "SELECT count(*) FROM iucn.expert_ranges_all WHERE lower(binomial) = ANY($1)");

	pqxx::work work(connection);
	pqxx::result result = work.prepared("occurrences")(taxa).exec();
	work.commit();

	return result[0][0].as<size_t>();
}

/**
 * read the GFBio data centers file and return as Json object
 * TODO: manage data centers in a database and map them to a c++ class
 * @return a json object containing the available data centers
 */
Json::Value GFBioDataUtil::getGFBioDataCentersJSON() {
	auto path = Configuration::get<std::string>("gfbio.abcd.datapath");

	std::ifstream file(path + "gfbio_datacenters.json");
	if (!file.is_open()) {
		throw std::runtime_error("gfbio_datacenters.json missing");
	}

	Json::Reader reader(Json::Features::strictMode());
	Json::Value root;
	if (!reader.parse(file, root)) {
		throw std::runtime_error("gfbio_datacenters.json invalid");
	}

	return root;
}


std::vector<std::string> GFBioDataUtil::getAvailableABCDArchives() {
	Json::Value dataCenters = getGFBioDataCentersJSON();
	std::vector<std::string> availableArchives;
	for(Json::Value &dataCenter : dataCenters["archives"]) {
		availableArchives.push_back(dataCenter.get("file", "").asString());
	}
	return availableArchives;
}

