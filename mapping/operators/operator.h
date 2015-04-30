#ifndef OPERATORS_OPERATOR_H
#define OPERATORS_OPERATOR_H

#include "datatypes/spatiotemporal.h"

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
class BinaryStream;

class QueryRectangle {
	public:
		QueryRectangle();
		QueryRectangle(time_t timestamp, double x1, double y1, double x2, double y2, uint32_t xres, uint32_t yres, epsg_t epsg) : timestamp(timestamp), x1(std::min(x1,x2)), y1(std::min(y1,y2)), x2(std::max(x1,x2)), y2(std::max(y1,y2)), xres(xres), yres(yres), epsg(epsg) {};
		QueryRectangle(const GridSpatioTemporalResult &grid);
		QueryRectangle(BinaryStream &stream);

		void toStream(BinaryStream &stream) const;

		double minx() const;
		double maxx() const;
		double miny() const;
		double maxy() const;

		void enlarge(int pixels);

		time_t timestamp;
		double x1, y1, x2, y2;
		uint32_t xres, yres;
		epsg_t epsg;
};

class QueryProfiler {
	public:
		QueryProfiler();
		QueryProfiler(const QueryProfiler& that) = delete;

		static double getTimestamp();

		double self_cpu;
		double all_cpu;
		double self_gpu;
		double all_gpu;
		size_t self_io;
		size_t all_io;
		// TODO: track GPU cost? Separately track things like Postgres queries?
		// TODO: track cached costs separately?

		void startTimer();
		void stopTimer();
		void addGPUCost(double seconds);
		void addIOCost(size_t bytes);

		QueryProfiler & operator+=(QueryProfiler &other);

	private:
		double t_start;
};

class GenericOperator {
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

		const std::string &getSemanticId() { return semantic_id; }

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

#define REGISTER_OPERATOR(classname, name) static std::unique_ptr<GenericOperator> create##classname(int sourcecounts[], GenericOperator *sources[], Json::Value &params) { return std::make_unique<classname>(sourcecounts, sources, params); } static OperatorRegistration register_##classname(name, create##classname)


#endif
