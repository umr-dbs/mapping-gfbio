#ifndef OPERATORS_OPERATOR_H
#define OPERATORS_OPERATOR_H

namespace Json {
	class Value;
}

class GenericRaster;
class PointCollection;
class GenericGeometry;
class Histogram;

class QueryRectangle {
	public:
		QueryRectangle();
		QueryRectangle(int timestamp, double x1, double y1, double x2, double y2, uint32_t xres, uint32_t yres, uint16_t epsg) : timestamp(timestamp), x1(x1), y1(y1), x2(x2), y2(y2), xres(xres), yres(yres), epsg(epsg) {};
		double minx() const;
		double maxx() const;
		double miny() const;
		double maxy() const;

		void enlarge(int pixels);

		int timestamp;
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
			HISTOGRAM
		};
		static const int MAX_SOURCES = 5;
		static GenericOperator *fromJSON(Json::Value &json);

		virtual ~GenericOperator();

		virtual GenericRaster *getRaster(const QueryRectangle &rect);
		virtual PointCollection *getPoints(const QueryRectangle &rect);
		virtual GenericGeometry *getGeometry(const QueryRectangle &rect);
		virtual Histogram *getHistogram(const QueryRectangle &rect);

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
		OperatorRegistration(const char *name, GenericOperator * (*constructor)(int sourcecount, GenericOperator *sources[], Json::Value &params));
};

#define REGISTER_OPERATOR(classname, name) static GenericOperator *create##classname(int sourcecount, GenericOperator *sources[], Json::Value &params) { return new classname(sourcecount, sources, params); } static OperatorRegistration register_##classname(name, create##classname)


#endif
