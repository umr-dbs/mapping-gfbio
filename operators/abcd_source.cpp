#include "operators/operator.h"
#include "datatypes/pointcollection.h"
#include "util/make_unique.h"
#include "util/exceptions.h"
#include "util/configuration.h"

#include <sstream>
#include <json/json.h>
#include <algorithm>
#include <string>
#include <functional>
#include <memory>
#include <cctype>
#include <unordered_set>
#include <vector>

#include <xercesc/parsers/XercesDOMParser.hpp>
#include <xercesc/parsers/DOMLSParserImpl.hpp>
#include <xercesc/dom/DOM.hpp>
#include <xercesc/dom/DOMConfiguration.hpp>
#include <xercesc/dom/impl/DOMElementNSImpl.hpp>
#include <xercesc/dom/impl/DOMTypeInfoImpl.hpp>
#include <xercesc/dom/DOMDocument.hpp>
#include <xercesc/dom/DOMPSVITypeInfo.hpp>
#include <xercesc/dom/DOMElement.hpp>
#include <xercesc/dom/DOMNodeList.hpp>
#include <xercesc/dom/DOMTypeInfo.hpp>
#include <xercesc/util/XMLString.hpp>
#include <xercesc/util/PlatformUtils.hpp>
#include <xercesc/util/TransService.hpp>
#include <xercesc/util/XMLException.hpp>
#include <xercesc/util/PSVIUni.hpp>
#include <xercesc/sax/SAXException.hpp>
#include <xercesc/dom/DOMException.hpp>
#include <xercesc/sax/HandlerBase.hpp>
#include <xercesc/framework/psvi/PSVIElement.hpp>
#include <xercesc/framework/psvi/XSModel.hpp>
#include <xercesc/framework/psvi/XSElementDeclaration.hpp>
#include <xercesc/framework/psvi/XSNamedMap.hpp>
#include <xercesc/framework/psvi/XSObject.hpp>
#include <xercesc/framework/psvi/PSVIHandler.hpp>
#include <xercesc/framework/psvi/PSVIAttributeList.hpp>
#include <xercesc/framework/psvi/XSAttributeDeclaration.hpp>
#include <xercesc/validators/schema/SchemaSymbols.hpp>

using namespace xercesc;

//define tag names of interest
static const XMLCh tagNameDataSet[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_D,chLatin_a,chLatin_t,chLatin_a,chLatin_S,chLatin_e,chLatin_t,chNull};
static const XMLCh tagNameUnits[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_U,chLatin_n,chLatin_i,chLatin_t,chLatin_s,chNull};
static const XMLCh tagNameUnit[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_U,chLatin_n,chLatin_i,chLatin_t,chNull};
static const XMLCh tagNameUnitID[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_U,chLatin_n,chLatin_i,chLatin_t,chLatin_I,chLatin_D,chNull};
static const XMLCh tagNameGathering[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_G,chLatin_a,chLatin_t,chLatin_h,chLatin_e,chLatin_r,chLatin_i,chLatin_n,chLatin_g,chNull};
static const XMLCh tagNameCoordinatesLatLong[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_C,chLatin_o,chLatin_o,chLatin_r,chLatin_d,chLatin_i,chLatin_n,chLatin_a,chLatin_t,chLatin_e,chLatin_s,chLatin_L,chLatin_a,chLatin_t,chLatin_L,chLatin_o,chLatin_n,chLatin_g,chNull};
static const XMLCh tagNameLongitudeDecimal[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_L,chLatin_o,chLatin_n,chLatin_g,chLatin_i,chLatin_t,chLatin_u,chLatin_d,chLatin_e,chLatin_D,chLatin_e,chLatin_c,chLatin_i,chLatin_m,chLatin_a,chLatin_l,chNull};
static const XMLCh tagNameLatitudeDecimal[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_L,chLatin_a,chLatin_t,chLatin_i,chLatin_t,chLatin_u,chLatin_d,chLatin_e,chLatin_D,chLatin_e,chLatin_c,chLatin_i,chLatin_m,chLatin_a,chLatin_l,chNull};
//static const XMLCh tagNameUnitGUID[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_U,chLatin_n,chLatin_i,chLatin_t,chLatin_G,chLatin_U,chLatin_I,chLatin_D,chNull};
static const XMLCh tagNameIPRStatements[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_I,chLatin_P,chLatin_R,chLatin_S,chLatin_t,chLatin_a,chLatin_t,chLatin_e,chLatin_m,chLatin_e,chLatin_n,chLatin_t,chLatin_s,chNull};
static const XMLCh tagNameCopyrights[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_C,chLatin_o,chLatin_p,chLatin_y,chLatin_r,chLatin_i,chLatin_g,chLatin_h,chLatin_t,chLatin_s,chNull};
static const XMLCh tagNameCopyright[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_C,chLatin_o,chLatin_p,chLatin_y,chLatin_r,chLatin_i,chLatin_g,chLatin_h,chLatin_t,chNull};
static const XMLCh tagNameLicenses[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_L,chLatin_i,chLatin_c,chLatin_e,chLatin_n,chLatin_s,chLatin_e,chLatin_s,chNull};
static const XMLCh tagNameLicense[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_L,chLatin_i,chLatin_c,chLatin_e,chLatin_n,chLatin_s,chLatin_e,chNull};
static const XMLCh tagNameURI[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_U,chLatin_R,chLatin_I,chNull};
static const XMLCh tagNameTitle[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_T,chLatin_i,chLatin_t,chLatin_l,chLatin_e,chNull};
static const XMLCh tagNameDetails[] = {chLatin_a,chLatin_b,chLatin_c,chLatin_d,chColon,chLatin_D,chLatin_e,chLatin_t,chLatin_a,chLatin_i,chLatin_l,chLatin_s,chNull};

/**
 * Operator that reads a given ABCD file and loads all units
 *
 * Parameters:
 * - archive: the path of the ABCD file
 * - units: an array with unit ddentifiers that specifies the units that are returned (optional)
 * 	- columns:
 * 		- numeric: array of column names of numeric type, XML path relative to DataSets/DataSet/Units/Unit
 * 		- textual: array of column names of textual type, XML path relative to DataSets/DataSet/Units/Unit
 */
class ABCDSourceOperator : public GenericOperator {
	public:
		ABCDSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, const QueryTools &tools);
		virtual void getProvenance(ProvenanceCollection &pc);
#endif
		void writeSemanticParameters(std::ostringstream& stream);

		virtual ~ABCDSourceOperator(){};

	private:
		std::string archive;
		std::string inputFile;

		bool filterUnitsById = false;
		std::unordered_set<std::string> units;

		std::vector<std::string> numeric_attributes;
		std::vector<std::string> textual_attributes;

#ifndef MAPPING_OPERATOR_STUBS
		std::unique_ptr<PointCollection> points;

		void handleUnits(DOMDocument& doc);
		void handleGlobalAttributes(DOMElement& dataSet);
		void handleUnit(DOMElement& unit);
		bool handleGathering(DOMElement& gathering);
		void handleXMLAttributes(DOMElement& element, std::string path,
				std::function<void(const std::string&, double)> setDoubleAttribute,
				std::function<void(const std::string&, const std::string&)> setStringAttribute);

		void handleIPRStatements(DOMElement& element, ProvenanceCollection &pc);


		double parseDouble(const XMLCh* text) const;

		void setFeatureStringAttribute(const std::string& attribute, const std::string& value);
		void setFeatureDoubleAttribute(const std::string& attribute, double value);

		void setGlobalStringAttribute(const std::string& attribute, const std::string& value);
		void setGlobalDoubleAttribute(const std::string& attribute, double value);

		std::unique_ptr<DOMLSParserImpl> createParser();

		std::string transcode(const XMLCh* ch) const;
		static const size_t TRANSCODE_BUFFER_SIZE = 16 * 1024;
		XMLTranscoder* transcoder = nullptr;

#endif

};
REGISTER_OPERATOR(ABCDSourceOperator, "abcd_source");



ABCDSourceOperator::ABCDSourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);
	archive = params.get("path", "").asString();

	// map archive url to local file
	std::stringstream ss;
	for(char& c : archive) {
		if (isalnum(c))
			ss << c;
		else
			ss << '_';
	}
	ss << ".xml";

	inputFile = ss.str();

	// filters on unitId
	if (params.isMember("units") && params["units"].size() > 0) {
		filterUnitsById = true;
		for (Json::Value &unit : params["units"]) {
			units.emplace(unit.asString());
		}
	}

	// attributes to be extracted
	if(!params.isMember("columns") || !params["columns"].isObject())
		throw ArgumentException("ABCDSourceOperator: columns are not specified");

	auto columns = params["columns"];
	if(!columns.isMember("numeric") || !columns["numeric"].isArray())
		throw ArgumentException("ABCDSourceOperator: numeric columns are not specified");

	if(!columns.isMember("textual") || !columns["textual"].isArray())
		throw ArgumentException("ABCDSourceOperator: textual columns are not specified");

	for(auto &attribute : columns["numeric"])
		numeric_attributes.push_back(attribute.asString());

	for(auto &attribute : columns["textual"])
		textual_attributes.push_back(attribute.asString());
}

void ABCDSourceOperator::writeSemanticParameters(std::ostringstream& stream) {
	Json::Value json(Json::objectValue);
	json["path"] = archive;

	// TODO: sort values to avoid unnecessary cache misses

	Json::Value jsonUnits(Json::arrayValue);
	for(auto &unit : units)
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

/**
 * Convert Xerces chars to string
 */
std::string ABCDSourceOperator::transcode(const XMLCh* ch) const {
	if (ch == nullptr)
		return "";

	XMLSize_t charsEaten = 0;
	char bytesNodeValue[TRANSCODE_BUFFER_SIZE];
	XMLSize_t charsReturned = transcoder->transcodeTo(ch,
			XMLString::stringLen(ch), (XMLByte*) bytesNodeValue, 4096,
			charsEaten, XMLTranscoder::UnRep_Throw);

	std::string string(bytesNodeValue, charsReturned);

	return string;
}

/**
 * parse double value from XMLCh
 */
double ABCDSourceOperator::parseDouble(const XMLCh* text) const {
	double result;
	try {
		result = std::stod(transcode(text));
	} catch (const std::invalid_argument& ia) {
		throw OperatorException(concat("ABCDSource: could not parse document:", inputFile));
	} catch (const std::out_of_range& oor) {
		throw OperatorException(concat("ABCDSource: could not parse document:", inputFile));
	}

	return result;
}

//datatype is considered numeric, if it is a restriction of xs:decimal
bool isNumeric(const DOMTypeInfo& info){
	return info.isDerivedFrom(PSVIUni::fgNamespaceXmlSchema, SchemaSymbols::fgDT_DECIMAL, DOMTypeInfo::DerivationMethods::DERIVATION_RESTRICTION);
}

/**
 * extract gathering for given unit
 * @return true if spatial info could be extracted (new feature created)
 */
bool ABCDSourceOperator::handleGathering(DOMElement& unit) {
	DOMNodeList* gatheringNodes = unit.getElementsByTagName(tagNameGathering);
	if(gatheringNodes->getLength() > 0) {
		DOMElement& gathering = dynamic_cast<DOMElement&>(*gatheringNodes->item(0));

		DOMNodeList* coordinates = gathering.getElementsByTagName(tagNameCoordinatesLatLong);

		bool addedCoordinates = false;
		auto size = coordinates->getLength();
		for (XMLSize_t coordinate = 0; coordinate < size; ++coordinate) {
			double x,y;

			DOMElement* coordinateElement = dynamic_cast<DOMElement*>(coordinates->item(coordinate));
			DOMNode* coordinateNode = coordinates->item(0);
			DOMNodeList* decimals = coordinateElement->getElementsByTagName(tagNameLongitudeDecimal);
			if (decimals->getLength() > 0) {
				x = parseDouble(decimals->item(0)->getFirstChild()->getNodeValue());
			} else {
				throw OperatorException(concat("ABCDSource: could not parse document:", inputFile));
			}

			decimals = coordinateElement->getElementsByTagName(tagNameLatitudeDecimal);
			if (decimals->getLength() > 0) {
				y = parseDouble(decimals->item(0)->getFirstChild()->getNodeValue());
			} else {
				throw OperatorException(concat("ABCDSource: could not parse document:", inputFile));
			}
			points->addCoordinate(x,y);
			addedCoordinates = true;
		}

		//TODO handle other gathering site infos

		//TODO handle info about gatherer....

		if(addedCoordinates) {
			points->finishFeature();
			return true;
		}
		return false;
	}
	return false;
}

/**
 * set string attribute for current feature
 */
void ABCDSourceOperator::setFeatureStringAttribute(const std::string& attribute, const std::string& value) {
	size_t position = points->start_feature.size() - 2;
	try {
		points->feature_attributes.textual(attribute).set(position, value);
	} catch (const std::out_of_range& e){
//		points->feature_attributes.addTextualAttribute(attribute, Unit::unknown());
//		points->feature_attributes.textual(attribute).set(position, value);
	}
}

/**
 * set double attribute for current feature
 */
void ABCDSourceOperator::setFeatureDoubleAttribute(const std::string& attribute, double value) {
	size_t position = points->start_feature.size() - 2;
	try {
		points->feature_attributes.numeric(attribute).set(position, value);
	} catch (const std::out_of_range& e){
//		points->feature_attributes.addNumericAttribute(attribute, Unit::unknown());
//		points->feature_attributes.numeric(attribute).set(position, value);
	}
}

void ABCDSourceOperator::setGlobalStringAttribute(const std::string& attribute, const std::string& value) {
	points->global_attributes.setTextual(attribute, value);
}


void ABCDSourceOperator::setGlobalDoubleAttribute(const std::string& attribute, double value) {
	points->global_attributes.setNumeric(attribute, value);
}



/**
 * recursively extract attributes from XML subtree by flattening
 */
void ABCDSourceOperator::handleXMLAttributes(DOMElement& element, std::string path,
		std::function<void(const std::string&, double)> setDoubleAttribute,
		std::function<void(const std::string&, const std::string&)> setStringAttribute) {

	path += transcode(element.getLocalName());
	if (element.hasAttributes()) {
		auto size = element.getAttributes()->getLength();
		for (XMLSize_t attribute = 0; attribute < size; ++attribute) {
			xercesc::DOMNode& attributeNode = *element.getAttributes()->item(
					attribute);
			xercesc::DOMAttr& attr = (xercesc::DOMAttr&) attributeNode;

			std::string attributePath = path + ":" + transcode(attr.getName());

			if (isNumeric(*attr.getSchemaTypeInfo())) {
				setDoubleAttribute(attributePath, parseDouble(attr.getValue()));
			} else {
				std::string value = transcode(attr.getValue());
				setStringAttribute(attributePath, value);
			}

		}
	}

	//visit children
	if (element.hasChildNodes()) {
		auto size = element.getChildNodes()->getLength();
		for (XMLSize_t child = 0; child < size; ++child) {

			DOMNode& childNode = *element.getChildNodes()->item(child);

			if (childNode.getNodeType()
					== xercesc::DOMNode::NodeType::ELEMENT_NODE) {
				handleXMLAttributes(dynamic_cast<DOMElement&>(childNode), path + "/", setDoubleAttribute,
						setStringAttribute);
			} else if (childNode.getNodeType() == xercesc::DOMNode::NodeType::TEXT_NODE) {
				xercesc::DOMText& text = (xercesc::DOMText&) childNode;
				if (!text.isIgnorableWhitespace()) {
					bool numeric = false;
					//have to check parent to find out datatype
					if (childNode.getParentNode()->getNodeType()
							== DOMNode::NodeType::ELEMENT_NODE) {
						DOMElement& parent =
								dynamic_cast<DOMElement&>(*childNode.getParentNode());
						numeric = isNumeric(*parent.getSchemaTypeInfo());
					}

					if (numeric) {
						setDoubleAttribute(path,
								parseDouble(childNode.getNodeValue()));
					} else {
						try {
							std::string value = transcode(
									childNode.getTextContent());
							setStringAttribute(path, value);
						} catch (...) {
							fprintf(stderr, "Error transcoding path %s\n",
									path.c_str());
						}
					}
				}
			} else {

			}

		}
	}


}

/**
 * handle given unit, extract and insert into point collection
 */
void ABCDSourceOperator::handleUnit(DOMElement& unit) {
	if (filterUnitsById) {
		// TODO: transcode unit filter ids to XMLCh* rather than all contained ids to string
		std::string unitId = transcode(unit.getElementsByTagName(tagNameUnitID)->item(0)->getTextContent());
		if (units.count(unitId) == 0)
			return;
	}
	bool newFeature = handleGathering(unit);

	std::function<void(const std::string&, double)> fn = std::bind(&ABCDSourceOperator::setFeatureDoubleAttribute, this, std::placeholders::_1, std::placeholders::_2);
	std::function<void(const std::string&, const std::string&)> fn2 = std::bind(&ABCDSourceOperator::setFeatureStringAttribute, this, std::placeholders::_1, std::placeholders::_2);


	if(newFeature) {
		//handle all elements of unit
		for (DOMElement* child = unit.getFirstElementChild(); child != nullptr; child = child->getNextElementSibling()){
//			if(XMLString::compareString(child->getTagName(), tagNameGathering) != 0) {
				handleXMLAttributes(*child, "", fn, fn2);
//			}
		}

		//missing attributes
		//TODO: introduce a proper check into attribute arrays
		for(std::string key : points->feature_attributes.getTextualKeys()){
			try {
				points->feature_attributes.textual(key).get(points->start_feature.size() - 2);
			} catch (const std::out_of_range& e) {
				points->feature_attributes.textual(key).set(points->start_feature.size() - 2, "");
			}
		}
		for(std::string key : points->feature_attributes.getNumericKeys()){
			try {
				points->feature_attributes.numeric(key).get(points->start_feature.size() - 2);
			} catch (const std::out_of_range& e) {
				points->feature_attributes.numeric(key).set(points->start_feature.size() - 2, 0.0);
			}
		}
	}
}

void ABCDSourceOperator::handleGlobalAttributes(DOMElement& dataSet){
	std::function<void(const std::string&, double)> setDoubleAttributeFn = std::bind(&ABCDSourceOperator::setGlobalDoubleAttribute, this, std::placeholders::_1, std::placeholders::_2);
	std::function<void(const std::string&, const std::string&)> setStringAttributeFn = std::bind(&ABCDSourceOperator::setGlobalStringAttribute, this, std::placeholders::_1, std::placeholders::_2);

	for(DOMElement* child = dataSet.getFirstElementChild(); child != nullptr; child = child->getNextElementSibling()) {
		if(XMLString::compareString(child->getTagName(), tagNameUnits) != 0){
			handleXMLAttributes(*child, "", setDoubleAttributeFn, setStringAttributeFn);
		}
	}
}

void ABCDSourceOperator::handleUnits(DOMDocument& doc) {
	DOMNodeList* units = doc.getElementsByTagName(tagNameUnit);

	auto size = units->getLength(); // don't recompute in loop, very expensive
	for(XMLSize_t unit = 0; unit < size; ++unit){
		DOMNode* unitNode = units->item(unit);
		DOMElement* unitElement = dynamic_cast<DOMElement*>(unitNode);
		handleUnit(*unitElement);
//		fprintf(stderr, "handled unit %d/%d\n", unit, size);
	}
}

std::unique_ptr<DOMLSParserImpl> ABCDSourceOperator::createParser() {
	//transcoder
	XMLTransService::Codes code;
	transcoder = XMLPlatformUtils::fgTransService->makeNewTranscoderFor("UTF-8", code, TRANSCODE_BUFFER_SIZE);

	if(code != XMLTransService::Codes::Ok)
		throw OperatorException("ABCDSource: could not create transcoder for UTF-8");

	//create parser
	static const XMLCh gLS[] = { chLatin_L, chLatin_S, chNull };
	DOMImplementation*  impl = DOMImplementationRegistry::getDOMImplementation(gLS); //deleted by XMLPlatform::Terminate
	std::unique_ptr<DOMLSParserImpl> parser(dynamic_cast<DOMLSParserImpl*>(impl->createLSParser(DOMImplementationLS::MODE_SYNCHRONOUS, 0)));

	//configure
	parser->setParameter(XMLUni::fgXercesDOMHasPSVIInfo, true);  //collect schema info
	parser->setParameter(XMLUni::fgDOMComments, false); //discard comments
	parser->setExternalNoNamespaceSchemaLocation("ABCD_2.06.XSD");
	parser->setDoSchema(true);
	parser->setValidationScheme(xercesc::XercesDOMParser::Val_Always);

	return parser;
}

std::unique_ptr<PointCollection> ABCDSourceOperator::getPointCollection(const QueryRectangle &rect, const QueryTools &tools){
	points = make_unique<PointCollection>(rect);

	// add attributes
	for(auto &attribute: numeric_attributes) {
		points->feature_attributes.addNumericAttribute(attribute, Unit::unknown());
	}

	for(auto &attribute: textual_attributes) {
		points->feature_attributes.addTextualAttribute(attribute, Unit::unknown());
	}

	//TODO: catch XML exceptions and throw OperatorException instead?

	XMLPlatformUtils::Initialize(); //TODO: only do this once for long running process
	{
		auto parser = createParser();
		std::string filePath = Configuration::get("gfbio.abcd.datapath") + "/" +inputFile;
		parser->parseURI(filePath.c_str());
		DOMDocument* doc = parser->getDocument(); //deleted by parser when done
		if(doc == nullptr)
			throw OperatorException(concat("ABCDSource: could not parse document (doc == null): ", filePath));

		//handle DataSet metadata
		DOMNodeList* dataSets = doc->getElementsByTagName(tagNameDataSet);
		if(dataSets->getLength() == 0)
			throw OperatorException(concat("ABCDSource: could not parse document no datasets found:", inputFile));
		handleGlobalAttributes(dynamic_cast<DOMElement&>(*dataSets->item(0)));
		//handle Units
		handleUnits(*doc);
	}
	delete transcoder; //transcoder has to be deleted before Terminate, thus wrapping member with unique_ptr not possible here
	//XMLPlatformUtils::Terminate(); //TODO: only do this once for long running process
	points->validate();
	return points->filterBySpatioTemporalReferenceIntersection(rect);
}

void ABCDSourceOperator::handleIPRStatements(DOMElement& element, ProvenanceCollection &pc) {
	Provenance provenance;
	provenance.local_identifier = "data." + getType() + "." + inputFile;

	//the IPR statements itself
	DOMElement* child = element.getFirstElementChild();
	while(child != nullptr) {
		if(XMLString::compareString(child->getNodeName(), tagNameCopyrights) == 0) {
			provenance.citation = transcode(child->getTextContent());
		} else if(XMLString::compareString(child->getNodeName(), tagNameLicenses) == 0) {
			provenance.license = transcode(child->getTextContent());
		} else if(XMLString::compareString(child->getNodeName(), tagNameURI) == 0) {
			provenance.uri = transcode(child->getTextContent());
		} else {
			//TODO terms of use?
		}
		child = child->getNextElementSibling();
	}

	//parent metadata the IPR statement belongs to
	DOMElement &metaData = dynamic_cast<DOMElement&>(*element.getParentNode());
	DOMNodeList* uriList = metaData.getElementsByTagName(tagNameURI);
	if(uriList->getLength() > 0) {
		DOMElement &uri = dynamic_cast<DOMElement&>(*uriList->item(0));
		provenance.uri = transcode(uri.getTextContent());
	}

	DOMNodeList* titleList = metaData.getElementsByTagName(tagNameTitle);
	if(titleList->getLength() > 0) {
		DOMElement &title = dynamic_cast<DOMElement&>(*titleList->item(0));
		provenance.citation += transcode(title.getTextContent()) + " ";
	}

	DOMNodeList* detailsList = metaData.getElementsByTagName(tagNameDetails);
	if(detailsList->getLength() > 0) {
		DOMElement &detail = dynamic_cast<DOMElement&>(*detailsList->item(0));
		provenance.citation += transcode(detail.getTextContent());
	}

	pc.add(provenance);
}

void ABCDSourceOperator::getProvenance(ProvenanceCollection &pc) {
	XMLPlatformUtils::Initialize(); //TODO: only do this once for long running process
	{
		auto parser = createParser();

		parser->parseURI((Configuration::get("gfbio.abcd.datapath") + "/" +inputFile).c_str());

		DOMDocument* doc = parser->getDocument(); //deleted by parser when done
		if(doc == nullptr)
			throw OperatorException(concat("ABCDSource: could not parse document:", inputFile));

		DOMNodeList* iprStatementsList = doc->getElementsByTagName(tagNameIPRStatements);
		auto size = iprStatementsList->getLength();
		for(size_t i = 0; i < size; ++i) {
			DOMElement& element = dynamic_cast<DOMElement&>(*iprStatementsList->item(i));
			handleIPRStatements(element, pc);
		}
	}
	delete transcoder;
	XMLPlatformUtils::Terminate(); //TODO: only do this once for long running process
}

#endif
