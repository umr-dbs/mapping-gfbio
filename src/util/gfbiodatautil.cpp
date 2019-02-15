
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
    for (size_t i = 0; i < result.size(); ++i) {
        if (i != 0)
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
    for (size_t i = 0; i < result.size(); ++i) {
        if (i != 0)
            taxa << ",";
        taxa << result[i][0];
    }
    taxa << "}";
    return taxa.str();
}

size_t GFBioDataUtil::countGBIFResults(std::string &scientificName) {
    pqxx::connection connection(Configuration::get<std::string>("operators.gfbiosource.dbcredentials"));

    std::string taxa = resolveTaxa(connection, scientificName);

    connection.prepare("occurrences",
                       "SELECT count(*) FROM gbif.gbif_lite_time WHERE taxon = ANY($1) AND geom IS NOT NULL");

    pqxx::work work(connection);
    pqxx::result result = work.prepared("occurrences")(taxa).exec();
    work.commit();

    return result[0][0].as<size_t>();
}

size_t GFBioDataUtil::countIUCNResults(std::string &scientificName) {
    pqxx::connection connection(Configuration::get<std::string>("operators.gfbiosource.dbcredentials"));

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
 * Structure:
 * [{
 * 		"link": <string>, "dataset": <string>, "file": <string>,
 * 		"provider": <string>, "available": <bool>,"isGeoReferenced": <bool>
 * }, ...]
 * @return a json object containing the available data centers
 */
Json::Value GFBioDataUtil::getGFBioDataCentersJSON() {
    pqxx::connection connection{Configuration::get<std::string>("operators.abcdsource.dbcredentials")};
    std::string schema = Configuration::get<std::string>("operators.abcdsource.schema");

    const auto view_table = "dataset_listing";
    connection.prepare(
            "abcd_info",
            concat(
                    "SELECT link, dataset, file, provider, available, isGeoReferenced",
                    " FROM ", schema, ".", view_table, ";"
            )
    );

    pqxx::work work(connection);
    pqxx::result result = work.prepared("abcd_info").exec();
    work.commit();

    Json::Value archives{Json::arrayValue};
    for (const auto &row : result) {
        Json::Value archive{Json::objectValue};

        archive["link"] = row["link"].as<std::string>();
        archive["dataset"] = row["dataset"].as<std::string>();
        archive["file"] = row["file"].as<std::string>();
        archive["provider"] = row["provider"].as<std::string>();
        archive["available"] = row["available"].as<bool>();
        archive["isGeoReferenced"] = row["isGeoReferenced"].as<bool>();

        archives.append(archive);
    }

    Json::Value root{Json::objectValue};
    root["archives"] = archives;
    return root;
}


std::vector<std::string> GFBioDataUtil::getAvailableABCDArchives() {
    pqxx::connection connection{Configuration::get<std::string>("operators.abcdsource.dbcredentials")};
    std::string schema = Configuration::get<std::string>("operators.abcdsource.schema");

    const auto view_table = "dataset_listing";
    connection.prepare(
            "abcd_info_available",
            concat("SELECT file FROM ", schema, ".", view_table, "WHERE available;")
    );

    pqxx::work work(connection);
    pqxx::result result = work.prepared("abcd_info_available").exec();
    work.commit();

    std::vector<std::string> files;
    for (const auto &row : result) {
        files.push_back(row["file"].as<std::string>());
    }

    return files;
}

