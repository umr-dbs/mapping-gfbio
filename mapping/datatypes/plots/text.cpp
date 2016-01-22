
#include "datatypes/plots/text.h"

#include <json/json.h>

TextPlot::TextPlot(const std::string &text) : text(text) {

}
TextPlot::~TextPlot() {

}

const std::string TextPlot::toJSON() const {
	Json::Value root(Json::ValueType::objectValue);

	root["type"] = "text";
	root["data"] = text;

	Json::FastWriter writer;
	return writer.write( root );
}
