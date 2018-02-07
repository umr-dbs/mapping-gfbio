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
        Parameter(const Json::Value &json, const std::vector<Parameter> &parameters);

        std::string name;
		std::string unit;
		bool numeric;

		bool isLongitudeColumn() const;
		bool isLatitudeColumn() const;

		Json::Value toJson();

		/**
		 * resolve name collisions with existing parameters
		 * @param parameters
		 */
		void handleNameCollision(const std::vector<Parameter> &parameters);
	};


	class MetaData {
	public:
		explicit MetaData(const Json::Value &json);

		enum class SpatialCoverageType {
			NONE, BOX, POINT
		};

		std::string title;
		std::vector<std::string> authors;
        std::string dataLink;
        std::string metaDataLink;
        std::string format;

		std::vector<Parameter> parameters;
		std::string spatialCoverageWKT;
		SpatialCoverageType spatialCoverageType;

		std::string license;
		std::string url;

		void initSpatialCoverage(const Json::Value &json);

        void parseFormat(const Json::Value &json);
    };

	static MetaData getMetaData(const std::string &dataSetDOI);

	static std::string getCitation(const std::string &dataSetDOI);

    static Json::Value getMetaDataFromPangaea(const std::string &dataSetDOI);

private:

    static std::vector<Parameter> parseParameters(const Json::Value &json);


};

#endif /* UTIL_PANGAEAAPI_H_ */
