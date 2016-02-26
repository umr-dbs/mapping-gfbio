#include "operators/operator.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/simplefeaturecollections/wkbutil.h"
#include "util/exceptions.h"
#include <json/json.h>

/**
 * Operator that takes Well-Known-Text as parameter
 */
class WKTSource : public GenericOperator {
	public:
	WKTSource(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources), params(params) {
			assumeSources(0);
			wkt = params.get("wkt", "").asString();
			type = params.get("type", "").asString();

			if(type != "points" && type != "lines" && type != "polygons")
				throw ArgumentException("WKTSource: Invalid type given");
		}

#ifndef MAPPING_OPERATOR_STUBS

		void setTime(SimpleFeatureCollection& collection) {
			auto& rect = collection.stref;

			if(params.isMember("time")){
				auto timeParam = params.get("time", Json::Value());
				if(timeParam.isArray()) {
					if(timeParam.size() != collection.getFeatureCount())
						throw ArgumentException("WKTSource: time array of invalid size given.");

					for(int i = 0; i < timeParam.size(); ++i){
						double t1, t2;

						auto entry = timeParam[i];
						auto start = entry[0];
						if(start.isString() && start.asString() == rect.ISO_BEGIN_OF_TIME)
							t1 = rect.beginning_of_time();
						else if(start.isNumeric())
							t1 = start.asDouble();
						else
							throw ArgumentException("WKTSource: end time is invalid");

						auto end = entry[1];
						if(end.isString() && end.asString() == rect.ISO_END_OF_TIME)
							t2 = rect.end_of_time();
						else if(end.isNumeric())
							t2 = end.asDouble();
						else
							throw ArgumentException("WKTSource: end time is invalid");

						collection.time.push_back(TimeInterval(t1, t2));
					}
				} else
					throw ArgumentException("WKTSource: time parameter is not an array.");
				collection.validate();
			}
		}

		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler){
			if(type != "points")
				throw ArgumentException("WKTSource does not contain points");
			auto points = WKBUtil::readPointCollection(wkt, rect);
			setTime(*points);
			return points->filterBySpatioTemporalReferenceIntersection(rect);
		}

		virtual std::unique_ptr<LineCollection> getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler){
			if(type != "lines")
				throw ArgumentException("WKTSource does not contain lines");
			auto lines = WKBUtil::readLineCollection(wkt, rect);
			setTime(*lines);
			return lines->filterBySpatioTemporalReferenceIntersection(rect);
		}

		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler){
			if(type != "polygons")
				throw ArgumentException("WKTSource does not contain polygons");
			auto polygons = WKBUtil::readPolygonCollection(wkt, rect);
			setTime(*polygons);
			return polygons->filterBySpatioTemporalReferenceIntersection(rect);
		}
#endif

		void writeSemanticParameters(std::ostringstream& stream) {
			Json::Value json;
			json["type"] = type;
			json["wkt"] = wkt;
			if(params.isMember("time"))
				json["time"] = params["time"];

			Json::FastWriter writer;
			stream << writer.write(json);
		}

		virtual ~WKTSource(){};

	private:
		std::string wkt;
		std::string type;
		Json::Value params;
};
REGISTER_OPERATOR(WKTSource, "wktsource");
