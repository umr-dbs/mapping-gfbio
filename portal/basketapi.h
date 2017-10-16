#ifndef PORTAL_BASKETAPI_H_
#define PORTAL_BASKETAPI_H_

#include <json/json.h>
#include "util/csv_source_util.h"

/**
 * This class encapsulates access to the Basket API of the GFBio portal
 */
class BasketAPI {
public:

	class Parameter {
	public:
        explicit Parameter(const Json::Value &json);
		Parameter(const std::string &name, const std::string& unit, bool numeric);

		std::string name;
		std::string unit;
		bool numeric;

		Json::Value toJson();
	};

	class BasketEntry {
	public:
		static std::unique_ptr<BasketAPI::BasketEntry> fromJson(const Json::Value &json, const std::vector<std::string> &availableArchives);

        explicit BasketEntry(const Json::Value &json);

        BasketEntry();

        enum class ResultType {
			NONE, POINTS, LINES, POLYGONS, RASTER
		};

		std::string title;
		std::vector<std::string> authors;

		std::string dataCenter;

		std::string metadataLink;
		std::string dataLink;

		std::vector<Parameter> parameters;

		bool available;

		ResultType resultType;

		virtual Json::Value toJson() const;

    };

	class ABCDBasketEntry : public BasketEntry {
	public:
		ABCDBasketEntry(const Json::Value &json, const std::vector<std::string> &availableArchives);

		std::string unitId;

        Json::Value toJson() const override;
	};

	class PangaeaBasketEntry : public BasketEntry {
	public:
        explicit PangaeaBasketEntry(const std::string &doi);

        std::string doi;
		std::string format;

		bool isTabSeparated;
		bool isGeoReferenced;

		GeometrySpecification geometrySpecification;
		std::string column_x;
		std::string column_y;

        Json::Value toJson() const override;
    };

	class Basket {
	public:
		Basket(const Json::Value &json, const std::vector<std::string> &availableArchives);

		std::string query;
		std::string timestamp;

		std::vector<std::unique_ptr<BasketEntry>> entries;

		Json::Value toJson() const;
	};

    class BasketOverview {
    public:
        explicit BasketOverview(const Json::Value &json);

        std::string query;
        std::string timestamp;
        size_t basketId;

        Json::Value toJson() const;
    };

	class BasketsOverview {
	public:
        explicit BasketsOverview(const Json::Value &json);

		size_t totalNumberOfBaskets;
		std::vector<BasketOverview> baskets;

		Json::Value toJson() const;
	};

	BasketAPI();
	virtual ~BasketAPI();

	static BasketsOverview getBaskets(const std::string &userId, size_t offset = 0, size_t limit = 10);
    static Basket getBasket(size_t basketId);

	struct BasketAPIException
			: public std::runtime_error { using std::runtime_error::runtime_error; };

};

#endif /* PORTAL_BASKETAPI_H_ */
