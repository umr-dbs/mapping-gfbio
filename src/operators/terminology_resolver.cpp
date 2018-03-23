
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"

#include "operators/operator.h"

#include <json/json.h>
#include "../util/terminology.h"

/**
 *
 * parameters:
 *  - column: name of the textual column to resolve
 *  - terminology: the terminology used to resolve
 *  - key: the json field to be returned
 *  - name_appendix: name of the next column for the resolved text values
 *      - "terminology": name will be "column terminology" with column and terminology being the parameters above
 *      - a custom string: name will be the custom string.
 *      - if not provided the name will be "column resolved" with column being the parameter above.
 *  - on_not_resolvable: if no label for the string was found, what to insert into new column
 *      - "EMPTY"
 *      - "OLD_NAME"
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
    std::string column;
    std::string terminology;
    std::string key;
    std::string new_column;
    HandleNotResolvable on_not_resolvable;

};

TerminologyResolver::TerminologyResolver(int *sourcecounts, GenericOperator **sources, Json::Value &params)
        : GenericOperator(sourcecounts, sources)
{
    column          = params.get("column", "").asString();
    terminology     = params.get("terminology", "").asString();
    if(terminology.find(',') != std::string::npos){
        throw ArgumentException("TerminologyResolver: Only one terminology should be requestet, not multiple concatenated by ','.");
    }
    key             = params.get("key", "label").asString();


    std::string name_appendix = params.get("name_appendix", "").asString();
    if(name_appendix.empty())
        new_column = column + " resolved";
    else if(name_appendix == "terminology")
        new_column = column + " " + terminology;
    else
        new_column = name_appendix;

    std::string not_resolvable = params.get("on_not_resolvable", "").asString();
    if(not_resolvable == "EMPTY")
        on_not_resolvable = EMPTY;
    else if(not_resolvable == "OLD_NAME")
        on_not_resolvable = OLD_NAME;
    else
        throw ArgumentException("Terminology Resolver: on_not_resolvable was not a valid value: " + not_resolvable + ". Must be EMPTY or OLD_NAME.");
}

TerminologyResolver::~TerminologyResolver() { }

REGISTER_OPERATOR(TerminologyResolver, "terminology_resolver");

std::unique_ptr<PointCollection>
TerminologyResolver::getPointCollection(const QueryRectangle &rect, const QueryTools &tools) {
    auto points = getPointCollectionFromSource(0, rect, tools);

    auto &old_attribute_array = points->feature_attributes.textual(column);
    auto &new_attribute_array = points->feature_attributes.addTextualAttribute(new_column, old_attribute_array.unit);
    Terminology::resolveMultiple(old_attribute_array.array, new_attribute_array.array, terminology, key,
                                 on_not_resolvable);

    return points;
}

std::unique_ptr<LineCollection>
TerminologyResolver::getLineCollection(const QueryRectangle &rect, const QueryTools &tools) {
    auto lines = getLineCollectionFromSource(0, rect, tools);

    auto &old_attribute_array = lines->feature_attributes.textual(column);
    auto &new_attribute_array = lines->feature_attributes.addTextualAttribute(new_column, old_attribute_array.unit);
    Terminology::resolveMultiple(old_attribute_array.array, new_attribute_array.array, terminology, key,
                                 on_not_resolvable);

    return lines;
}

std::unique_ptr<PolygonCollection>
TerminologyResolver::getPolygonCollection(const QueryRectangle &rect, const QueryTools &tools) {
    auto polygons = getPolygonCollectionFromSource(0, rect, tools);

    auto &old_attribute_array = polygons->feature_attributes.textual(column);
    auto &new_attribute_array = polygons->feature_attributes.addTextualAttribute(new_column, old_attribute_array.unit);
    Terminology::resolveMultiple(old_attribute_array.array, new_attribute_array.array, terminology, key,
                                 on_not_resolvable);

    return polygons;
}

void TerminologyResolver::writeSemanticParameters(std::ostringstream &stream) {
    Json::Value json(Json::objectValue);

    json["column"]              = column;
    json["terminology"]         = terminology;
    json["key"]                 = key;
    json["new_column"]          = new_column;
    json["on_not_resolvable"]   = (on_not_resolvable == EMPTY) ? "EMPTY" : "NOT_EMPTY";

    stream << json;
}

