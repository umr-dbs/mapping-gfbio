/*
 * cheat.h
 *
 *  Created on: 15.01.2016
 *      Author: mika
 */

#ifndef EXPERIMENTS_CHEAT_H_
#define EXPERIMENTS_CHEAT_H_

#include "datatypes/simplefeaturecollection.h"
#include "util/timemodification.h"
#include "util/gdal.h"

class ProjectionOperator : public GenericOperator {
	friend class QuerySpec;
	public:
		ProjectionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~ProjectionOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<LineCollection> getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream &stream);
	private:
		QueryRectangle projectQueryRectangle(const QueryRectangle &rect, const GDAL::CRSTransformer &transformer);
		epsg_t src_epsg, dest_epsg;
};

class TimeShiftOperator : public GenericOperator {
	friend class QuerySpec;
	public:
		TimeShiftOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~TimeShiftOperator();

		virtual auto getRaster(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<GenericRaster>;
		virtual auto getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<PointCollection>;
		virtual auto getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<LineCollection>;
		virtual auto getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<PolygonCollection>;

	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		/**
		 * Creates the time modification.
		 *
		 * @param temporal_reference The temporal reference of the query.
		 */
		auto createTimeModification(const TemporalReference& temporal_reference) -> TimeModification;

		/**
		 * Shift a {@ref QueryRectangle} by using the time_modification.
		 *
		 * @param time_modification A TimeModification object.
		 * @param rect a query rectangle reference
		 *
		 * @return a shifted query rectagle
		 */
		inline auto shift(TimeModification& time_modification, const QueryRectangle& rect) -> const QueryRectangle;

		/**
		 * Reverses the shift on a {@ref SpatioTemporalResult} by using the time_modification.
		 *
		 * @param time_modification A TimeModification object.
		 * @param result A spatio temporal result reference
		 */
		inline auto reverse(TimeModification& time_modification, SpatioTemporalResult& result) -> void;

		/**
		 * Reverses the shift on the elements of a {@ref SimpleFeatureCollection} by using the time_modification.
		 *
		 * @param time_modification A TimeModification object.
		 * @param collection A simple feature collection
		 */
		inline auto reverseElements(TimeModification& time_modification, SimpleFeatureCollection& collection) -> void;

		bool shift_has_from = false, shift_has_to = false;
		std::string shift_from_unit, shift_from_value;
		std::string shift_to_unit, shift_to_value;
		bool has_stretch = false;
		int stretch_factor;
		std::string stretch_fixed_point;
		bool snap_has_from = false, snap_has_to = false;
		std::string snap_from_unit, snap_to_unit;
		int snap_from_value, snap_to_value;
		bool snap_from_allow_reset, snap_to_allow_reset;
};



#endif /* EXPERIMENTS_CHEAT_H_ */
