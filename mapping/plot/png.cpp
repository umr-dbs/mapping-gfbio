
#include "plot/png.h"

#include <util/base64.h>
#include <json/json.h>

PNGPlot::PNGPlot(const std::string &binary) : binary(binary) {

}
PNGPlot::~PNGPlot() {

}

std::string PNGPlot::toJSON() {
	Json::Value root(Json::ValueType::objectValue);

	root["type"] = "png";
	root["data"] = base64_encode(binary);

	Json::FastWriter writer;
	return writer.write( root );
}
