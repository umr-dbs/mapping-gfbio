#include "operators/operator.h"
#include "datatypes/pointcollection.h"
#include "util/make_unique.h"
#include "util/exceptions.h"
#include "util/configuration.h"
#include "util/stringsplit.h"

#include <sstream>
#include <json/json.h>
#include <algorithm>
#include <string>
#include <functional>
#include <memory>
#include <cctype>
#include <unordered_set>
#include <vector>
#include <pugixml.hpp>
#include <iostream>
#include <numeric>
#include <pqxx/pqxx>


/**
 * Operator that reads a given ABCD file and loads all units
 *
 * Parameters:
 * - archive: the path of the ABCD file
 * - units: an array with unit identifiers that specifies the units that are returned (optional)
 * - columns:
 * 		- numeric: array of column names of numeric type, XML path relative to DataSets/DataSet/Units/Unit
 * 		- textual: array of column names of textual type, XML path relative to DataSets/DataSet/Units/Unit
 */
class ABCDSourceOperator : public GenericOperator {
    public:
        ABCDSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);

#ifndef MAPPING_OPERATOR_STUBS

        std::unique_ptr<PointCollection>
        getPointCollection(const QueryRectangle &rect, const QueryTools &tools) override;

        void getProvenance(ProvenanceCollection &pc) override;

#endif

        void writeSemanticParameters(std::ostringstream &stream) override;

        ~ABCDSourceOperator() override = default;;

    private:
        std::string archive;

        bool filterUnitsById = false;
        std::unordered_set<std::string> unitIds;

#ifndef MAPPING_OPERATOR_STUBS
        std::vector<std::string> numeric_attributes;
        std::vector<std::string> numeric_attribute_hashes;
        std::vector<std::string> textual_attributes;
        std::vector<std::string> textual_attribute_hashes;

        std::stringstream unit_filter;

        std::unique_ptr<PointCollection> createFeatureCollectionWithAttributes(const QueryRectangle &rect);

#endif
};

REGISTER_OPERATOR(ABCDSourceOperator, "abcd_source");

auto hash(const std::string &string) -> std::string {
    SHA1 hasher;
    hasher.addBytes(string);
    return hasher.digest().asHex();
}

// TODO: extract to core util
// TODO: std::accumulate
template<typename T>
auto join(const T &strings, const std::string &delimiter) -> std::string {
    std::stringstream result;
    bool first = true;
    for (const auto &string : strings) {
        if (first) {
            result << string;
            first = false;
        } else {
            result << delimiter << string;
        }
    }
    return result.str();
}

ABCDSourceOperator::ABCDSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params)
        : GenericOperator(sourcecounts, sources) {
    assumeSources(0);
    archive = params.get("path", "").asString();

    // filters on unitId
    const auto unit_id_field = "/DataSets/DataSet/Units/Unit/UnitID";
    const auto unit_id_field_hash = hash(unit_id_field);
    if (params.isMember("units") && !params["units"].empty()) {
        filterUnitsById = true;
        for (Json::Value &unit : params["units"]) {
            unitIds.emplace(unit.asString());
            unit_filter << unit.asString() << "','";
        }
        this->unit_filter << unit_id_field_hash << "{'";
        this->unit_filter << join(unitIds, "','");
        this->unit_filter << "'}";
    } else {
        this->unit_filter << "true";
    }

    // attributes to be extracted
    if (!params.isMember("columns") || !params["columns"].isObject())
        throw ArgumentException("ABCDSourceOperator: columns are not specified");

    auto columns = params["columns"];
    if (!columns.isMember("numeric") || !columns["numeric"].isArray())
        throw ArgumentException("ABCDSourceOperator: numeric columns are not specified");

    if (!columns.isMember("textual") || !columns["textual"].isArray())
        throw ArgumentException("ABCDSourceOperator: textual columns are not specified");

    for (auto &attribute : columns["numeric"]) {
        auto field = attribute.asString();
        numeric_attributes.push_back(field);
        numeric_attribute_hashes.push_back(hash(field));
    }

    for (auto &attribute : columns["textual"]) {
        auto field = attribute.asString();
        textual_attributes.push_back(field);
        textual_attribute_hashes.push_back(hash(field));
    }
}

void ABCDSourceOperator::writeSemanticParameters(std::ostringstream &stream) {
    Json::Value json(Json::objectValue);
    json["path"] = archive;

    // TODO: sort values to avoid unnecessary cache misses

    Json::Value jsonUnits(Json::arrayValue);
    for (auto &unit : unitIds)
        jsonUnits.append(unit);
    json["units"] = jsonUnits;


    Json::Value columns(Json::objectValue);

    Json::Value jsonNumeric(Json::arrayValue);
    for (auto &attribute : numeric_attributes)
        jsonNumeric.append(attribute);
    columns["numeric"] = jsonNumeric;

    Json::Value jsonTextual(Json::arrayValue);
    for (auto &attribute : textual_attributes)
        jsonTextual.append(attribute);
    columns["textual"] = jsonTextual;

    json["columns"] = columns;

    stream << json;
}

#ifndef MAPPING_OPERATOR_STUBS

std::unique_ptr<PointCollection> ABCDSourceOperator::createFeatureCollectionWithAttributes(const QueryRectangle &rect) {
    auto points = make_unique<PointCollection>(rect);

    for (auto &attribute : numeric_attributes) {
        points->feature_attributes.addNumericAttribute(attribute, Unit::unknown());
    }

    for (auto &attribute : textual_attributes) {
        points->feature_attributes.addTextualAttribute(attribute, Unit::unknown());
    }

    return points;
}

std::unique_ptr<PointCollection>
ABCDSourceOperator::getPointCollection(const QueryRectangle &rect, const QueryTools &tools) {
    // TODO: global attributes
    // TODO: connection pooling

    const auto longitude_column = "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LongitudeDecimal";
    const auto latitude_column = "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LatitudeDecimal";
    const auto longitude_column_hash = hash(longitude_column);
    const auto latitude_column_hash = hash(latitude_column);

    const auto unit_id_column_hash = hash("/DataSets/DataSet/Units/Unit/UnitID");

    pqxx::connection connection{Configuration::get("operators.abcdsource.dbcredentials")};
    std::string schema = Configuration::get("operators.abcdsource.schema");

    const auto numeric_columns = join(numeric_attribute_hashes, "\",\"");
    const auto textual_columns = join(textual_attribute_hashes, "\",\"");

    const auto MAX_RETURN_ITEMS = 100000;
    const auto TABLE_SAMPLE_SEED = .618651;

    connection.prepare(
            "abcd_query",
            concat(
                    "WITH JOINED_TBL AS ( ",
                    "SELECT *"
                    "FROM ", schema, ".abcd_datasets JOIN ", schema, ".abcd_units USING(surrogate_key) ",
                    "WHERE dataset_id = $1 ",
                    "AND ", filterUnitsById ? concat(unit_id_column_hash, " IN ($2) ") : "$2 ",
                    "AND \"", longitude_column_hash, "\" IS NOT NULL ",
                    "AND \"", latitude_column_hash, "\" IS NOT NULL ",
                    "AND \"", longitude_column_hash, "\" BETWEEN $3 and $4 ",
                    "AND \"", latitude_column_hash, "\" BETWEEN $5 and $6 ",
                    ") ",
                    "SELECT ",
                    "\"", longitude_column_hash, "\",\"", latitude_column_hash, "\"",
                    numeric_columns.empty() ? "" : ",\"",
                    numeric_columns,
                    numeric_columns.empty() ? "" : "\"",
                    textual_columns.empty() ? "" : ",\"",
                    textual_columns,
                    textual_columns.empty() ? "" : "\"",
                    " ",
                    "FROM JOINED_TBL "
                    "WHERE RANDOM()<=(", MAX_RETURN_ITEMS, "::float / (SELECT COUNT(*)::float FROM JOINED_TBL)) ",
                    ";"
            )
    );
    pqxx::work work{connection};
    work.exec(concat("SELECT SETSEED(", TABLE_SAMPLE_SEED, ")")); // set seed for random
    pqxx::result result = work.prepared("abcd_query")
                    (archive)
                    (unit_filter.str())
                    (rect.x1)(rect.x2)
                    (rect.y1)(rect.y2)
            .exec();
    work.commit();

    auto points = createFeatureCollectionWithAttributes(rect);

    for (const auto &row : result) {
        // coordinates
        auto x = row[longitude_column_hash].as<double>();
        auto y = row[latitude_column_hash].as<double>();
        points->addSinglePointFeature(Coordinate(x, y));

        // attributes
        for (int i = 0; i < numeric_attributes.size(); ++i) {
            const auto attribute = numeric_attributes[i];
            const auto hash = numeric_attribute_hashes[i];
            const auto entry = row[hash];
            // TODO: default value? null value?
            auto value = entry.is_null() ? NAN : row[hash].as<double>();
            points->feature_attributes.numeric(attribute).set(points->getFeatureCount() - 1, value);
        }
        for (int i = 0; i < textual_attributes.size(); ++i) {
            const auto attribute = textual_attributes[i];
            const auto hash = textual_attribute_hashes[i];
            const auto entry = row[hash];
            // TODO: default value? null value?
            auto value = entry.is_null() ? "" : row[hash].as<std::string>();
            points->feature_attributes.textual(attribute).set(points->getFeatureCount() - 1, value);
        }
    }

    return points;
}


void ABCDSourceOperator::getProvenance(ProvenanceCollection &pc) {
    const auto title_path = "/DataSets/DataSet/Metadata/Description/Representation/Title";
    const auto citation_path = "/DataSets/DataSet/Metadata/IPRStatements/Citations/Citation/Text";
    const auto uri_path = "/DataSets/DataSet/Metadata/Description/Representation/URI";
    const auto license_path = "/DataSets/DataSet/Metadata/IPRStatements/Licenses/License/Text";

    const auto title_path_hash = hash(title_path);
    const auto citation_path_hash = hash(citation_path);
    const auto uri_path_hash = hash(uri_path);
    const auto license_path_hash = hash(license_path);

    pqxx::connection connection{Configuration::get("operators.abcdsource.dbcredentials")};
    std::string schema = Configuration::get("operators.abcdsource.schema");

    connection.prepare(
            "abcd_provenance",
            concat(
                    "SELECT ",
                    "\"", citation_path_hash, "\", ",
                    "\"", uri_path_hash, "\", ",
                    "\"", license_path_hash, "\" ",
                    "FROM ", schema, ".abcd_datasets ",
                    "WHERE dataset_id = $1 ",
                    ";"
            )
    );
    pqxx::work work{connection};
    pqxx::result result = work.prepared("abcd_provenance")(archive).exec();
    work.commit();

    if (result.empty()) {
        throw ArgumentException(concat("The ABCD dataset ", archive, "does not exist."));
    }

    const auto row = result[0]; // single row result

    Provenance provenance;
    provenance.local_identifier = "data." + getType();

    provenance.citation = row[citation_path_hash].is_null() ? "" : row[citation_path_hash].as<std::string>();
    provenance.uri = row[uri_path_hash].is_null() ? "" : row[uri_path_hash].as<std::string>();
    provenance.license = row[license_path_hash].is_null() ? "" : row[license_path_hash].as<std::string>();

    pc.add(provenance);
}

#endif
