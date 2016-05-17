
#include "operators/provenance.h"

#include <json/json.h>
#include <unordered_set>


Provenance::Provenance(const std::string &citation, const std::string &license, const std::string &uri, const std::string &local_identifier)
	: citation(citation), license(license), uri(uri), local_identifier(local_identifier) {
	// TODO: do we need
}





void ProvenanceCollection::add(const Provenance &provenance) {
	// TODO: do we want or need to filter duplicates?
	items.push_back(provenance);
}

std::string ProvenanceCollection::toJson() {
	Json::Value result(Json::ValueType::arrayValue);
	for (const auto &p : items) {
		Json::Value item(Json::ValueType::objectValue);
		item["citation"] = p.citation;
		item["license"] = p.license;
		item["uri"] = p.uri;
		item["local_identifier"] = p.local_identifier; // TODO: do we want to export this?
		result.append(item);
	}

	Json::FastWriter writer;
	return writer.write(result);
}

std::vector<std::string> ProvenanceCollection::getLocalIdentifiers() {
	std::unordered_set<std::string> set;
	for (const auto &p : items) {
		if (p.local_identifier != "")
			set.insert(p.local_identifier);
	}

	std::vector<std::string> result;
	for (const auto &s : set)
		result.push_back(s);

	return result;
}
