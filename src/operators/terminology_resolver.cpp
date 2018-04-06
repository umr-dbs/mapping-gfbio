
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "operators/operator.h"
#include <json/json.h>
#include "util/terminology.h"

/**
 * Operator for resolving attributes using the terminology service from gfbio (search api): https://terminologies.gfbio.org/
 *
 * parameters:
 *  - attribute_name: name of the textual attribute to resolve.
 *  - resolved_attribute: name of the new attribute for the resolved terms.
 *  - terminology: name of the terminology used to resolve.
 *  - key: the json field of the result to be saved in the resolved attribute. "label" if not provided.
 *         if requested field is an array, the first element will be returned.
 *  - match_type: "exact", "included", "regex", see TerminologyService search API. "exact" if not provided.
 *  - first_hit: bool, see TerminologyService search API. true if not provided.
 *  - on_not_resolvable: if no label for the term was found, what to insert into resolved attribute.
 *      - "EMPTY" inserts an empty string
 *      - "KEEP" inserts the original term
 */

class TerminologyResolver : public GenericOperator {
public:
    TerminologyResolver(int *sourcecounts, GenericOperator **sources, Json::Value &params);
    ~TerminologyResolver() override;

    std::unique_ptr<PointCollection>    getPointCollection(const QueryRectangle &rect, const QueryTools &tools) override;
    std::unique_ptr<LineCollection>     getLineCollection(const QueryRectangle &rect, const QueryTools &tools) override;
    std::unique_ptr<PolygonCollection>  getPolygonCollection(const QueryRectangle &rect, const QueryTools &tools) override;

protected:
    void writeSemanticParameters(std::ostringstream& stream) override;

private:
    std::string attribute_name;
    std::string terminology;
    std::string key;
    std::string resolved_attribute;
    std::string match_type;
    bool first_hit;
    HandleNotResolvable on_not_resolvable;

};

TerminologyResolver::TerminologyResolver(int *sourcecounts, GenericOperator **sources, Json::Value &params)
        : GenericOperator(sourcecounts, sources)
{
    terminology         = params.get("terminology", "").asString();
    if(terminology.find(',') != std::string::npos){
        throw ArgumentException("TerminologyResolver: Only one terminology should be requested, not multiple concatenated by ','.");
    }
    attribute_name      = params.get("attribute_name", "").asString();
    key                 = params.get("key", "label").asString();
    resolved_attribute  = params.get("resolved_attribute", "").asString();
    if(resolved_attribute == attribute_name){
        throw OperatorException("Terminology Resolver: name of resolved attribute has to be different from existing attribute.");
    }

    std::string not_resolvable = params.get("on_not_resolvable", "").asString();
    if(not_resolvable == "EMPTY")
        on_not_resolvable = HandleNotResolvable::EMPTY;
    else if(not_resolvable == "KEEP")
        on_not_resolvable = HandleNotResolvable::KEEP;
    else
        throw ArgumentException("Terminology Resolver: on_not_resolvable was not a valid value: " + not_resolvable + ". Must be EMPTY or KEEP.");

    match_type = params.get("match_type", "exact").asString();
    if(match_type != "exact" && match_type != "included" && match_type != "regex")
        throw ArgumentException("Terminology Resolver: unknown match_type (must be exact, included or regex) -> " + match_type);

    first_hit  = params.get("first_hit", "true").asBool();
}

TerminologyResolver::~TerminologyResolver() { }

REGISTER_OPERATOR(TerminologyResolver, "terminology_resolver");

std::unique_ptr<PointCollection>
TerminologyResolver::getPointCollection(const QueryRectangle &rect, const QueryTools &tools) {
    auto points = getPointCollectionFromSource(0, rect, tools);

    // with AttributeArray being private, with no direct access to the underlying vector the strings have
    // to be copied into a new vector first.
    // the AttributeArray can not be passed to Terminology, because the class is private.

    auto &old_attribute_array = points->feature_attributes.textual(attribute_name);
    auto &new_attribute_array = points->feature_attributes.addTextualAttribute(resolved_attribute, old_attribute_array.unit);

    std::vector<std::string> names_in;
    size_t feature_count = points->getFeatureCount();
    names_in.reserve(feature_count);

    for(int i = 0; i < feature_count; i++){
        names_in.push_back(old_attribute_array.get(i));
    }

    auto names_out = Terminology::resolveMultiple(names_in, terminology, key, match_type, first_hit, on_not_resolvable);

    new_attribute_array.reserve(names_out.size());

    // insert the resolved strings into the new attribute array.
    for(int i = 0; i < names_out.size(); i++){
        new_attribute_array.set(i, names_out[i]);
    }

    return points;
}

std::unique_ptr<LineCollection>
TerminologyResolver::getLineCollection(const QueryRectangle &rect, const QueryTools &tools) {
    auto lines = getLineCollectionFromSource(0, rect, tools);

    auto &old_attribute_array = lines->feature_attributes.textual(attribute_name);
    auto &new_attribute_array = lines->feature_attributes.addTextualAttribute(resolved_attribute, old_attribute_array.unit);

    std::vector<std::string> names_in;
    size_t feature_count = lines->getFeatureCount();
    names_in.reserve(feature_count);

    for(int i = 0; i < feature_count; i++){
        names_in.push_back(old_attribute_array.get(i));
    }

    auto names_out = Terminology::resolveMultiple(names_in, terminology, key, match_type, first_hit, on_not_resolvable);

    new_attribute_array.reserve(names_out.size());

    // insert the resolved strings into the new attribute array.
    for(int i = 0; i < names_out.size(); i++){
        new_attribute_array.set(i, names_out[i]);
    }

    return lines;
}

std::unique_ptr<PolygonCollection>
TerminologyResolver::getPolygonCollection(const QueryRectangle &rect, const QueryTools &tools) {
    auto polygons = getPolygonCollectionFromSource(0, rect, tools);

    auto &old_attribute_array = polygons->feature_attributes.textual(attribute_name);
    auto &new_attribute_array = polygons->feature_attributes.addTextualAttribute(resolved_attribute, old_attribute_array.unit);

    std::vector<std::string> names_in;
    size_t feature_count = polygons->getFeatureCount();
    names_in.reserve(feature_count);

    for(int i = 0; i < feature_count; i++){
        names_in.push_back(old_attribute_array.get(i));
    }

    auto names_out = Terminology::resolveMultiple(names_in, terminology, key, match_type, first_hit, on_not_resolvable);

    new_attribute_array.reserve(names_out.size());

    // insert the resolved strings into the new attribute array.
    for(int i = 0; i < names_out.size(); i++){
        new_attribute_array.set(i, names_out[i]);
    }

    return polygons;
}

void TerminologyResolver::writeSemanticParameters(std::ostringstream &stream) {
    Json::Value json(Json::objectValue);

    json["attribute_name"]      = attribute_name;
    json["resolved_attribute"]  = resolved_attribute;
    json["terminology"]         = terminology;
    json["key"]                 = key;
    json["match_type"]          = match_type;
    json["first_hit"]           = first_hit;
    json["on_not_resolvable"]   = (on_not_resolvable == HandleNotResolvable::EMPTY) ? "EMPTY" : "KEEP";

    stream << json;
}

