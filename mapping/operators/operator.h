#ifndef OPERATORS_OPERATOR_H
#define OPERATORS_OPERATOR_H

#include "datatypes/spatiotemporal.h"
#include "operators/queryrectangle.h"
#include "operators/queryprofiler.h"

#include <ctime>
#include <string>
#include <sstream>
#include <memory>
#include "util/make_unique.h"

namespace Json {
	class Value;
}

class GenericRaster;
class PointCollection;
class LineCollection;
class PolygonCollection;
class GenericPlot;


class GenericOperator {
	friend class CacheManager;
	public:
		enum class RasterQM {
			EXACT,
			LOOSE
		};

		enum class FeatureCollectionQM {
			ANY_FEATURE,
			SINGLE_ELEMENT_FEATURES
		};

		static const int MAX_INPUT_TYPES = 4;
		static const int MAX_SOURCES = 20;
		static std::unique_ptr<GenericOperator> fromJSON(const std::string &json, int depth = 0);
		static std::unique_ptr<GenericOperator> fromJSON(Json::Value &json, int depth = 0);

		virtual ~GenericOperator();

		std::unique_ptr<GenericRaster> getCachedRaster(const QueryRectangle &rect, QueryProfiler &profiler, RasterQM query_mode = RasterQM::LOOSE);
		std::unique_ptr<PointCollection> getCachedPointCollection(const QueryRectangle &rect, QueryProfiler &profiler, FeatureCollectionQM query_mode = FeatureCollectionQM::ANY_FEATURE);
		std::unique_ptr<LineCollection> getCachedLineCollection(const QueryRectangle &rect, QueryProfiler &profiler, FeatureCollectionQM query_mode = FeatureCollectionQM::ANY_FEATURE);
		std::unique_ptr<PolygonCollection> getCachedPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler, FeatureCollectionQM query_mode = FeatureCollectionQM::ANY_FEATURE);
		std::unique_ptr<GenericPlot> getCachedPlot(const QueryRectangle &rect, QueryProfiler &profiler);

		const std::string &getSemanticId() const { return semantic_id; }
		int getDepth() const { return depth; }

	protected:
		GenericOperator(int sourcecounts[], GenericOperator *sources[]);
		virtual void writeSemanticParameters(std::ostringstream &stream);
		void assumeSources(int rasters, int pointcollections=0, int linecollections=0, int polygoncollections=0);

		int getRasterSourceCount() { return sourcecounts[0]; }
		int getPointCollectionSourceCount() { return sourcecounts[1]; }
		int getLineCollectionSourceCount() { return sourcecounts[2]; }
		int getPolygonCollectionSourceCount() { return sourcecounts[3]; }

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<LineCollection> getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect, QueryProfiler &profiler);

		std::unique_ptr<GenericRaster> getRasterFromSource(int idx, const QueryRectangle &rect, QueryProfiler &profiler, RasterQM query_mode = RasterQM::LOOSE);
		std::unique_ptr<PointCollection> getPointCollectionFromSource(int idx, const QueryRectangle &rect, QueryProfiler &profiler, FeatureCollectionQM query_mode = FeatureCollectionQM::ANY_FEATURE);
		std::unique_ptr<LineCollection> getLineCollectionFromSource(int idx, const QueryRectangle &rect, QueryProfiler &profiler, FeatureCollectionQM query_mode = FeatureCollectionQM::ANY_FEATURE);
		std::unique_ptr<PolygonCollection> getPolygonCollectionFromSource(int idx, const QueryRectangle &rect, QueryProfiler &profiler, FeatureCollectionQM query_mode = FeatureCollectionQM::ANY_FEATURE);
		// there is no getPlotFromSource, because plots are by definition the final step of a chain

	private:
		int sourcecounts[MAX_INPUT_TYPES];
		GenericOperator *sources[MAX_SOURCES];

		std::string type;
		std::string semantic_id;
		int depth;

		void operator=(GenericOperator &) = delete;
};

class OperatorRegistration {
	public:
		OperatorRegistration(const char *name, std::unique_ptr<GenericOperator> (*constructor)(int sourcecounts[], GenericOperator *sources[], Json::Value &params));
};

#define REGISTER_OPERATOR(classname, name) static std::unique_ptr<GenericOperator> create##classname(int sourcecounts[], GenericOperator *sources[], Json::Value &params) { return make_unique<classname>(sourcecounts, sources, params); } static OperatorRegistration register_##classname(name, create##classname)


#endif
