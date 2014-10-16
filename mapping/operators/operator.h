#ifndef OPERATORS_OPERATOR_H
#define OPERATORS_OPERATOR_H

#include <ctime>
#include <string>
#include <memory>
#include "util/make_unique.h"

namespace Json {
	class Value;
}

class GenericRaster;
class PointCollection;
class GenericGeometry;
class GenericPlot;
class BinaryStream;

class QueryRectangle {
	public:
		QueryRectangle();
		QueryRectangle(time_t timestamp, double x1, double y1, double x2, double y2, uint32_t xres, uint32_t yres, uint16_t epsg) : timestamp(timestamp), x1(x1), y1(y1), x2(x2), y2(y2), xres(xres), yres(yres), epsg(epsg) {};
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
		uint16_t epsg;
};

class GenericOperator {
	public:
		static const int MAX_INPUT_TYPES = 3;
		static const int MAX_SOURCES = 20;
		static std::unique_ptr<GenericOperator> fromJSON(const std::string &json);
		static std::unique_ptr<GenericOperator> fromJSON(Json::Value &json);

		virtual ~GenericOperator();

		virtual std::unique_ptr<GenericRaster> getCachedRaster(const QueryRectangle &rect);
		virtual std::unique_ptr<PointCollection> getCachedPoints(const QueryRectangle &rect);
		virtual std::unique_ptr<GenericGeometry> getCachedGeometry(const QueryRectangle &rect);
		virtual std::unique_ptr<GenericPlot> getCachedPlot(const QueryRectangle &rect);

	protected:
		GenericOperator(int sourcecounts[], GenericOperator *sources[]);
		void assumeSources(int rasters, int pointcollections=0, int geometries=0);

		int getRasterSourceCount() { return sourcecounts[0]; }
		int getPointsSourceCount() { return sourcecounts[1]; }
		int getGeometrySourceCount() { return sourcecounts[2]; }

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect);
		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
		virtual std::unique_ptr<GenericGeometry> getGeometry(const QueryRectangle &rect);
		virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect);

		std::unique_ptr<GenericRaster> getRasterFromSource(int idx, const QueryRectangle &rect);
		std::unique_ptr<PointCollection> getPointsFromSource(int idx, const QueryRectangle &rect);
		std::unique_ptr<GenericGeometry> getGeometryFromSource(int idx, const QueryRectangle &rect);
		// there is no getPlotFromSource, because plots are by definition the final step of a chain

	private:
		int sourcecounts[MAX_INPUT_TYPES];
		GenericOperator *sources[MAX_SOURCES];

		void operator=(GenericOperator &) = delete;
};


class OperatorRegistration {
	public:
		OperatorRegistration(const char *name, std::unique_ptr<GenericOperator> (*constructor)(int sourcecounts[], GenericOperator *sources[], Json::Value &params));
};

#define REGISTER_OPERATOR(classname, name) static std::unique_ptr<GenericOperator> create##classname(int sourcecounts[], GenericOperator *sources[], Json::Value &params) { return std::make_unique<classname>(sourcecounts, sources, params); } static OperatorRegistration register_##classname(name, create##classname)


#endif
