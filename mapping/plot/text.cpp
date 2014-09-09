
#include "plot/text.h"

#include <json/json.h>

TextPlot::TextPlot(const std::string &text) : text(text) {

}
TextPlot::~TextPlot() {

}

std::string TextPlot::toJSON() {
	Json::Value root(Json::ValueType::objectValue);

	root["type"] = "text";
	root["data"] = text;

	Json::FastWriter writer;
	return writer.write( root );
}
