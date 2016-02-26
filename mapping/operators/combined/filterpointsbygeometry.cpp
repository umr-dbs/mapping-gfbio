#include "datatypes/raster.h"
#include "datatypes/raster/raster_priv.h"
#include "operators/operator.h"
#include "datatypes/pointcollection.h"
#include "datatypes/polygoncollection.h"
#include "util/make_unique.h"

#include <string>
#include <sstream>
#include <algorithm>


/**
 * Filter simple pointcollection by a polygoncollection
 */
class FilterPointsByGeometry : public GenericOperator {
	public:
		FilterPointsByGeometry(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~FilterPointsByGeometry();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);

		std::unique_ptr<PointCollection> filterWithTime(const QueryRectangle &rect, PointCollection &points, PolygonCollection &multiPolygons);
#endif
};




FilterPointsByGeometry::FilterPointsByGeometry(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(2);
}

FilterPointsByGeometry::~FilterPointsByGeometry() {
}
REGISTER_OPERATOR(FilterPointsByGeometry, "filterpointsbygeometry");


#ifndef MAPPING_OPERATOR_STUBS

std::unique_ptr<PointCollection> FilterPointsByGeometry::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto points = getPointCollectionFromSource(0, rect, profiler, FeatureCollectionQM::SINGLE_ELEMENT_FEATURES);
	auto multiPolygons = getPolygonCollectionFromSource(0, rect, profiler, FeatureCollectionQM::ANY_FEATURE);

	if(!points->hasTime() && !multiPolygons->hasTime()) {
		//filter only based on geometry
		auto tester = multiPolygons->getPointInCollectionBulkTester();

		size_t points_count = points->getFeatureCount();
		std::vector<bool> keep(points_count, false);

		for(auto feature : *points){
			for(auto& coordinate : feature){
				if(tester.pointInCollection(coordinate)){
					keep[feature] = true;
					break;
				}
			}
		}

		return points->filter(keep);
	} else {
		if(!points->hasTime())
			points->addDefaultTimestamps();

		if(!multiPolygons->hasTime())
			multiPolygons->addDefaultTimestamps();

		return filterWithTime(rect, *points, *multiPolygons);

	}
}

std::unique_ptr<PointCollection> FilterPointsByGeometry::filterWithTime(const QueryRectangle &rect, PointCollection& points, PolygonCollection& multiPolygons) {
	//initialize new point collection
	auto points_out = make_unique<PointCollection>(rect);
	points_out->addGlobalAttributesFromCollection(points);
	points_out->addFeatureAttributesFromCollection(points);

	auto tester = multiPolygons.getPointInCollectionBulkTester();

	auto textualAttributes = points.feature_attributes.getTextualKeys();
	auto numericAttributes = points.feature_attributes.getNumericKeys();

	for(auto feature : points){
		std::vector<TimeInterval> intervals;

		//TODO: for multi-points: gather polygons for all point. But have to clarify semantics first.
		auto polygons = tester.polygonsContainingPoint(*feature.begin());

		//gather all time intervals in which feature intersects with a polygon
		for(uint32_t polygon : polygons){
			if(points.time[feature].intersects(multiPolygons.time[polygon])) {
				TimeInterval intersection = points.time[feature].intersection(multiPolygons.time[polygon]);
				intervals.push_back(intersection);
			}
		}

		//merge overlapping time intervals
		std::sort(intervals.begin(), intervals.end(), [=] (const TimeInterval &a, const TimeInterval &b) -> bool {
			return a.t1 < b.t1;
		});

		size_t i = 1;
		while(i < intervals.size()){
			if(intervals[i-1].intersects(intervals[i])) {
				intervals[i-1].union_with(intervals[i]);
				intervals.erase(intervals.begin() + i);
			} else {
				++i;
			}
		}

		//create new points
		for(TimeInterval interval : intervals){
			points_out->addFeatureFromCollection(points, feature, textualAttributes, numericAttributes);
			size_t index = points_out->getFeatureCount() - 1;
			points_out->time[index] = interval;
		}
	}
	return points_out;
}



#endif

