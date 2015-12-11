#ifndef UNITTESTS_SIMPLEFEATURECOLLECTIONS_UTIL_H_
#define UNITTESTS_SIMPLEFEATURECOLLECTIONS_UTIL_H_

#include <gtest/gtest.h>
#include "datatypes/pointcollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/linecollection.h"

class CollectionTestUtil {
public:
	static void checkAttributeMapsEquality(const AttributeMaps& expected, const AttributeMaps& actual){
		{//numeric
			auto expectedRef = expected.numeric();
			auto actualRef = actual.numeric();

			auto expectedIt = expectedRef.begin();
			auto actualIt = actualRef.begin();

			for(;expectedIt != expectedRef.end() && actualIt !=actualRef.end(); ++expectedIt, ++actualIt){
				EXPECT_EQ(expectedIt->first, actualIt->first);
				EXPECT_EQ(expectedIt->second, actualIt->second);
			}
			EXPECT_EQ(expectedRef.end(), expectedIt);
			EXPECT_EQ(actualRef.end(), actualIt);
		}

		{//textual
			auto expectedRef = expected.textual();
			auto actualRef = actual.textual();

			auto expectedIt = expectedRef.begin();
			auto actualIt = actualRef.begin();

			for(;expectedIt != expectedRef.end() && actualIt !=actualRef.end(); ++expectedIt, ++actualIt){
				EXPECT_EQ(expectedIt->first, actualIt->first);
				EXPECT_EQ(expectedIt->second, actualIt->second);
			}
			EXPECT_EQ(expectedRef.end(), expectedIt);
			EXPECT_EQ(actualRef.end(), actualIt);
		}
	}

	static void checkAttributeArraysEquality(const AttributeArrays &expected, const AttributeArrays &actual, size_t featureCount){
		{//numeric
			auto expectedKeys = expected.getNumericKeys();
			auto actualKeys = actual.getNumericKeys();

			EXPECT_EQ(expectedKeys.size(), actualKeys.size());
			for(size_t keyIndex = 0; keyIndex < expectedKeys.size(); ++keyIndex){
				std::string& expectedKey = expectedKeys[keyIndex];
				std::string& actualKey = actualKeys[keyIndex];
				EXPECT_EQ(expectedKey, actualKey);
				for(size_t i = 0; i < featureCount; ++i){
					EXPECT_EQ(expected.numeric(expectedKey).get(i), actual.numeric(actualKey).get(i));
				}
			}
		}

		{//textual
			auto expectedKeys = expected.getTextualKeys();
			auto actualKeys = actual.getTextualKeys();

			EXPECT_EQ(expectedKeys.size(), actualKeys.size());
			for(size_t keyIndex = 0; keyIndex < expectedKeys.size(); ++keyIndex){
				std::string& expectedKey = expectedKeys[keyIndex];
				std::string& actualKey = actualKeys[keyIndex];
				EXPECT_EQ(expectedKey, actualKey);
				for(size_t i = 0; i < featureCount; ++i){
					EXPECT_EQ(expected.textual(expectedKey).get(i), actual.textual(actualKey).get(i));
				}
			}
		}
	}

	static void checkStrefEquality(const SpatioTemporalReference &expected, const SpatioTemporalReference &actual){
		EXPECT_EQ(expected.epsg, actual.epsg);
		EXPECT_EQ(expected.timetype, actual.timetype);
		EXPECT_EQ(expected.t1, actual.t1);
		EXPECT_EQ(expected.t2, actual.t2);
		EXPECT_EQ(expected.epsg, actual.epsg);
		EXPECT_EQ(expected.x1, actual.x1);
		EXPECT_EQ(expected.y1, actual.y1);
		EXPECT_EQ(expected.x2, actual.x2);
		EXPECT_EQ(expected.y2, actual.y2);
	}

	static void checkSimpleFeatureCollectionEquality(const SimpleFeatureCollection &expected, const SimpleFeatureCollection &actual){
		checkStrefEquality(expected.stref, actual.stref);

		//check global attributes
		checkAttributeMapsEquality(actual.global_attributes, expected.global_attributes);

		//check time
		EXPECT_EQ(expected.getFeatureCount(), actual.getFeatureCount());
		EXPECT_EQ(expected.hasTime(), actual.hasTime());
	}

	static void checkEquality(const PointCollection& expected, const PointCollection& actual){
		checkSimpleFeatureCollectionEquality(actual, expected);

		//check features
		for(size_t feature = 0; feature < expected.getFeatureCount(); ++feature){
			EXPECT_EQ(expected.getFeatureReference(feature).size(), actual.getFeatureReference(feature).size());
			if(expected.hasTime()){
				EXPECT_EQ(expected.time_start[feature], actual.time_start[feature]);
				EXPECT_EQ(expected.time_end[feature], actual.time_end[feature]);
			}

			//check feature attributes equality
			checkAttributeArraysEquality(expected.feature_attributes, actual.feature_attributes, expected.getFeatureCount());

			for(size_t point = expected.start_feature[feature]; point < expected.start_feature[feature + 1]; ++point){
				EXPECT_EQ(expected.coordinates[point].x, actual.coordinates[point].x);
				EXPECT_EQ(expected.coordinates[point].y, actual.coordinates[point].y);
			}
		}
	}

	static void checkEquality(const LineCollection& expected, const LineCollection& actual){
		checkSimpleFeatureCollectionEquality(actual, expected);

		for(size_t feature = 0; feature < expected.getFeatureCount(); ++feature){
			EXPECT_EQ(expected.getFeatureReference(feature).size(), actual.getFeatureReference(feature).size());
			if(expected.hasTime()){
				EXPECT_EQ(expected.time_start[feature], actual.time_start[feature]);
				EXPECT_EQ(expected.time_end[feature], actual.time_end[feature]);
			}

			//check feature attributes equality
			checkAttributeArraysEquality(expected.feature_attributes, actual.feature_attributes, expected.getFeatureCount());

			for(size_t line = 0; line < expected.getFeatureReference(feature).size(); ++line){
				EXPECT_EQ(expected.getFeatureReference(feature).getLineReference(line).size(), actual.getFeatureReference(feature).getLineReference(line).size());

				for(size_t point = expected.start_line[expected.getFeatureReference(feature).getLineReference(line).getLineIndex()];
						point < expected.start_line[expected.getFeatureReference(feature).getLineReference(line).getLineIndex() + 1]; ++point){
					EXPECT_EQ(expected.coordinates[point].x, actual.coordinates[point].x);
					EXPECT_EQ(expected.coordinates[point].y, actual.coordinates[point].y);
				}
			}
		}
	}

	static void checkEquality(const PolygonCollection& expected, const PolygonCollection& actual){
		checkSimpleFeatureCollectionEquality(expected, actual);

		for(size_t feature = 0; feature < expected.getFeatureCount(); ++feature){
			EXPECT_EQ(expected.getFeatureReference(feature).size(), actual.getFeatureReference(feature).size());
			if(expected.hasTime()){
				EXPECT_EQ(expected.time_start[feature], actual.time_start[feature]);
				EXPECT_EQ(expected.time_end[feature], actual.time_end[feature]);
			}

			//check feature attributes equality
			checkAttributeArraysEquality(expected.feature_attributes, actual.feature_attributes, expected.getFeatureCount());

			for(size_t polygon = 0; polygon < expected.getFeatureReference(feature).size(); ++polygon){
				EXPECT_EQ(expected.getFeatureReference(feature).getPolygonReference(polygon).size(), actual.getFeatureReference(feature).getPolygonReference(polygon).size());
				for(size_t ring = 0; ring < expected.getFeatureReference(feature).getPolygonReference(polygon).size(); ++ring){
					EXPECT_EQ(expected.getFeatureReference(feature).getPolygonReference(polygon).getRingReference(ring).size(), actual.getFeatureReference(feature).getPolygonReference(polygon).getRingReference(ring).size());

					for(size_t point = expected.start_ring[expected.getFeatureReference(feature).getPolygonReference(polygon).getRingReference(ring).getRingIndex()];
							point < expected.start_ring[expected.getFeatureReference(feature).getPolygonReference(polygon).getRingReference(ring).getRingIndex() + 1]; ++point){
						EXPECT_EQ(expected.coordinates[point].x, actual.coordinates[point].x);
						EXPECT_EQ(expected.coordinates[point].y, actual.coordinates[point].y);
					}
				}
			}
		}
	}
};

#endif /* UNITTESTS_SIMPLEFEATURECOLLECTIONS_UTIL_H_ */
