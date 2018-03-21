
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
 *  - name_appendix:
 *      - "terminology": name will be "column terminology" with column and terminology being the parameters above
 *      - a custom_string: name will be "column custom_string" with column being the parameter above.
 *      - if not provided the name will be "column resolved" with column being the parameter above.
 *  - on_not_resolvable: if no label for the string was found, what to insert into new column
 *      - "EMPTY"
 *      - "OLD_NAME"
 */

class TerminologyResolver : public GenericOperator {
public:
    TerminologyResolver(int *sourcecounts, GenericOperator **sources, Json::Value &params);

    std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, const QueryTools &tools) override;

    std::unique_ptr<LineCollection> getLineCollection(const QueryRectangle &rect, const QueryTools &tools) override;

    std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, const QueryTools &tools) override;

private:
    std::string column;
    std::string terminology;
    std::string new_column;
    HandleNotResolvable on_not_resolvable;

};

REGISTER_OPERATOR(TerminologyResolver, "terminology_resolver");


TerminologyResolver::TerminologyResolver(int *sourcecounts, GenericOperator **sources, Json::Value &params)
        : GenericOperator(sourcecounts, sources) {
    column          = params.get("column", "").asString();
    terminology     = params.get("terminology", "").asString();

    std::string name_appendix = params.get("name_appendix", "").asString();

    if(name_appendix.empty())
        new_column = column + " resolved";
    else if(name_appendix == "terminology")
        new_column = column + " " + terminology;
    else
        new_column = column + " " + name_appendix;

    std::string not_resolvable = params.get("on_not_resolvable", "").asString();
    if(not_resolvable == "EMPTY")
        on_not_resolvable = EMPTY;
    else if(not_resolvable == "OLD_NAME")
        on_not_resolvable = OLD_NAME;
    else
        throw ArgumentException("Terminology Resolver: on_not_resolvable has not valid value: " + not_resolvable);
}

std::unique_ptr<PointCollection>
TerminologyResolver::getPointCollection(const QueryRectangle &rect, const QueryTools &tools) {

    auto points = getPointCollectionFromSource(0, rect, tools);

    auto &old_attribute_array = points->feature_attributes.textual(column);
    //create new textual column and fill it with resolved labels from the Terminology class
    auto &new_vector = points->feature_attributes.addTextualAttribute(new_column, old_attribute_array.unit);
    Terminology::requestLabels(old_attribute_array.array, new_vector.array, terminology, on_not_resolvable);

    return points;
}

std::unique_ptr<LineCollection>
TerminologyResolver::getLineCollection(const QueryRectangle &rect, const QueryTools &tools) {

    auto lines = getLineCollectionFromSource(0, rect, tools);

    auto &old_attribute_array = lines->feature_attributes.textual(column);
    //create new textual column and fill it with resolved labels from the Terminology class
    auto &new_vector = lines->feature_attributes.addTextualAttribute(new_column, old_attribute_array.unit);
    Terminology::requestLabels(old_attribute_array.array, new_vector.array, terminology, on_not_resolvable);

    return lines;
}

std::unique_ptr<PolygonCollection>
TerminologyResolver::getPolygonCollection(const QueryRectangle &rect, const QueryTools &tools) {
    auto polygons = getPolygonCollectionFromSource(0, rect, tools);

    auto &old_attribute_array = polygons->feature_attributes.textual(column);
    //create new textual column and fill it with resolved labels from the Terminology class
    auto &new_vector = polygons->feature_attributes.addTextualAttribute(new_column, old_attribute_array.unit);
    Terminology::requestLabels(old_attribute_array.array, new_vector.array, terminology, on_not_resolvable);

    return polygons;
}
