#ifndef UTIL_PANGAEAAPI_H_
#define UTIL_PANGAEAAPI_H_

#include <vector>
#include <json/json.h>

/**
 * This class encapsulated access to the PangaeaAPI for retrieving
 * data set meta data
 */
class PangaeaAPI {
public:

	class Parameter {
	public:
		Parameter(const Json::Value &json);

		std::string name;
		std::string unit;
		bool numeric;

		bool isLongitudeColumn() const;
		bool isLatitudeColumn() const;

		Json::Value toJson();
	};


	class MetaData {
	public:
		MetaData(const Json::Value &json);

		enum class SpatialCoverageType {
			NONE, BOX, POINT
		};

		std::vector<Parameter> parameters;
		std::string spatialCoverageWKT;
		SpatialCoverageType spatialCoverageType;

		std::string license;
		std::string url;

		void initSpatialCoverage(const Json::Value &json);

	private:
		static std::string parseSpatialCoverage(const Json::Value &json);
	};

	static std::vector<Parameter> getParameters(const std::string &dataSetDOI);

	static MetaData getMetaData(const std::string &dataSetDOI);

	static std::string getCitation(const std::string &dataSetDOI);

private:
	static Json::Value getMetaDataFromPangaea(const std::string &dataSetDOI);

	static std::vector<Parameter> parseParameters(const Json::Value &json);


};

#endif /* UTIL_PANGAEAAPI_H_ */
