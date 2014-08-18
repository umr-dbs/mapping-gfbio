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
class DataVector;

class QueryRectangle {
	public:
		QueryRectangle();
		QueryRectangle(time_t timestamp, double x1, double y1, double x2, double y2, uint32_t xres, uint32_t yres, uint16_t epsg) : timestamp(timestamp), x1(x1), y1(y1), x2(x2), y2(y2), xres(xres), yres(yres), epsg(epsg) {};
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
		enum class Type {
			RASTER,
			POINTS,
			GEOMETRY,
			DATAVECTOR
		};
		static const int MAX_SOURCES = 5;
		static std::unique_ptr<GenericOperator> fromJSON(const std::string &json);
		static std::unique_ptr<GenericOperator> fromJSON(Json::Value &json);

		virtual ~GenericOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect);
		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
		virtual std::unique_ptr<GenericGeometry> getGeometry(const QueryRectangle &rect);
		virtual std::unique_ptr<DataVector> getDataVector(const QueryRectangle &rect);

	protected:
		GenericOperator(Type type, int sourcecount, GenericOperator *sources[]);
		Type type;
		int sourcecount;
		GenericOperator *sources[MAX_SOURCES];

		void assumeSources(int n);

	private:
		void operator=(GenericOperator &) = delete;
};


class OperatorRegistration {
	public:
		OperatorRegistration(const char *name, std::unique_ptr<GenericOperator> (*constructor)(int sourcecount, GenericOperator *sources[], Json::Value &params));
};

#define REGISTER_OPERATOR(classname, name) static std::unique_ptr<GenericOperator> create##classname(int sourcecount, GenericOperator *sources[], Json::Value &params) { return std::make_unique<classname>(sourcecount, sources, params); } static OperatorRegistration register_##classname(name, create##classname)


#endif
