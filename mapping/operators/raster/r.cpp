
#include "raster/raster.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "operators/operator.h"


#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <mutex>
#include <functional>
#include <cmath>


#include <json/json.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter" // silence the myriad of warnings in ***REMOVED*** headers

#include <***REMOVED***Common.h>

// These template declarations must be between ***REMOVED***Common.h and ***REMOVED***.h
// The implementation follows later
namespace ***REMOVED*** {
	// QueryRectangle
	template<> SEXP wrap(const QueryRectangle &rect);
	template<> QueryRectangle as(SEXP sexp);

	// Raster
	template<> SEXP wrap(const GenericRaster &raster);
	template<> SEXP wrap(const std::unique_ptr<GenericRaster> &raster);
	template<> std::unique_ptr<GenericRaster> as(SEXP sexp);
}

#include <***REMOVED***.h>
#include <***REMOVED***.h>

#pragma clang diagnostic pop // ignored "-Wunused-parameter"


class ROperator : public GenericOperator {
	public:
		ROperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~ROperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect);
		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
		virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect);

		SEXP runScript(const QueryRectangle &rect);
	private:
		friend std::unique_ptr<GenericRaster> query_raster_source(ROperator *op, int childidx, const QueryRectangle &rect);

		std::string source;
		std::string result_type;
};

ROperator::ROperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	source = params["source"].asString();
	result_type = params["result"].asString();
}
ROperator::~ROperator() {
}
REGISTER_OPERATOR(ROperator, "r");


static std::mutex r_running_mutex;

std::unique_ptr<GenericRaster> query_raster_source(ROperator *op, int childidx, const QueryRectangle &rect) {
	auto raster = op->getRasterFromSource(childidx, rect);
	raster->setRepresentation(GenericRaster::Representation::CPU);
	return raster;
}

SEXP ROperator::runScript(const QueryRectangle &rect) {
	std::lock_guard<std::mutex> guard(r_running_mutex);
	Profiler::Profiler p("R_OPERATOR");

	Profiler::start("R_OPERATOR: construct");
	***REMOVED*** R;
	Profiler::stop("R_OPERATOR: construct");

	Profiler::start("R_OPERATOR: load libraries");
	R.parseEvalQ("library(\"raster\")");
	Profiler::stop("R_OPERATOR: load libraries");

	Profiler::start("R_OPERATOR: environment");
	std::function<std::unique_ptr<GenericRaster>(int, const QueryRectangle &)> bound_source = std::bind(query_raster_source, this, std::placeholders::_1, std::placeholders::_2);
	R["mapping.rastercount"] = getRasterSourceCount();
	R["mapping.loadRaster"] = ***REMOVED***::InternalFunction( bound_source );;
	R["mapping.qrect"] = rect;
	Profiler::stop("R_OPERATOR: environment");

	Profiler::start("R_OPERATOR: executing script");
	auto result = R.parseEval(source);
	Profiler::stop("R_OPERATOR: executing script");

	return result;
}


std::unique_ptr<GenericRaster> ROperator::getRaster(const QueryRectangle &rect) {
	if (result_type != "raster")
		throw OperatorException("This R script does not return rasters");

	return ***REMOVED***::as<std::unique_ptr<GenericRaster>>(runScript(rect));
}

std::unique_ptr<PointCollection> ROperator::getPoints(const QueryRectangle &rect) {
	if (result_type != "points")
		throw OperatorException("This R script does not return a point collection");

	throw OperatorException("TODO");
	//return runScript(rect);
}

std::unique_ptr<GenericPlot> ROperator::getPlot(const QueryRectangle &rect) {
	if (result_type != "plot")
		throw OperatorException("This R script does not return a plot");

	throw OperatorException("TODO");
	//return runScript(rect);
}




namespace ***REMOVED*** {
	// QueryRectangle
	template<> SEXP wrap(const QueryRectangle &rect) {
		Profiler::Profiler p("R_OPERATOR: wrapping qrect");
		***REMOVED***::List list;

		list["timestamp"] = rect.timestamp;
		list["x1"] = rect.x1;
		list["y1"] = rect.y1;
		list["x2"] = rect.x2;
		list["y2"] = rect.y2;
		list["xres"] = rect.xres;
		list["yres"] = rect.xres;
		list["epsg"] = rect.epsg;

		return ***REMOVED***::wrap(list);
	}
	template<> QueryRectangle as(SEXP sexp) {
		Profiler::Profiler p("R_OPERATOR: unwrapping qrect");
		***REMOVED***::List list = ***REMOVED***::as<***REMOVED***::List>(sexp);

		return QueryRectangle(
			list["timestamp"],
			list["x1"],
			list["y1"],
			list["x2"],
			list["y2"],
			list["xres"],
			list["yres"],
			list["epsg"]
		);
	}

	// Raster
	template<> SEXP wrap(const GenericRaster &raster) {
		Profiler::Profiler p("R_OPERATOR: wrapping raster");
		int width = raster.lcrs.size[0];
		int height = raster.lcrs.size[1];

		***REMOVED***::NumericVector pixels(raster.lcrs.getPixelCount());
		int pos = 0;
		for (int y=0;y<height;y++) {
			for (int x=0;x<width;x++) {
				double val = raster.getAsDouble(x, y);
				if (raster.dd.is_no_data(val))
					pixels[pos++] = NAN;
				else
					pixels[pos++] = val;
			}
		}

		***REMOVED***::S4 data(".SingleLayerData");
		data.slot("values") = pixels;
		data.slot("inmemory") = true;
		data.slot("fromdisk") = false;
		data.slot("haveminmax") = true;
		data.slot("min") = raster.dd.min;
		data.slot("max") = raster.dd.max;

		***REMOVED***::S4 extent("Extent");
		extent.slot("xmin") = raster.lcrs.origin[0];
		extent.slot("ymin") = raster.lcrs.origin[1];
		extent.slot("xmax") = raster.lcrs.PixelToWorldX(raster.lcrs.size[0]);
		extent.slot("ymax") = raster.lcrs.PixelToWorldY(raster.lcrs.size[1]);

		***REMOVED***::S4 crs("CRS");
		std::ostringstream epsg;
		epsg << "EPSG:" << raster.lcrs.epsg;
		crs.slot("projargs") = epsg.str();

		***REMOVED***::S4 rasterlayer("RasterLayer");
		rasterlayer.slot("data") = data;
		rasterlayer.slot("extent") = extent;
		rasterlayer.slot("crs") = crs;
		rasterlayer.slot("nrows") = raster.lcrs.size[0];
		rasterlayer.slot("ncols") = raster.lcrs.size[1];

		return ***REMOVED***::wrap(rasterlayer);

		/*
		 * Thomas sagt: "Raster" und "Spatial Data Frame"?
		 */

/*
class       : RasterLayer
dimensions  : 180, 360, 64800  (nrow, ncol, ncell)
resolution  : 1, 1  (x, y)
extent      : -180, 180, -90, 90  (xmin, xmax, ymin, ymax)
coord. ref. : +proj=longlat +datum=WGS84

attributes(r)
$history: list()
$file: class .RasterFile
$data: class .SingleLayerData  (hat haveminmax, min, max!)
$legend: class .RasterLegend
$title: character(0)
$extent: class Extent (xmin, xmax, ymin, ymax)
$rotated: FALSE
$rotation: class .Rotation
$ncols: int
$nrows: int
$crs: CRS arguments: +proj=longlat +datum=WGS84
$z: list()
$class: c("RasterLayer", "raster")

 */
	}
	template<> SEXP wrap(const std::unique_ptr<GenericRaster> &raster) {
		return ***REMOVED***::wrap(*raster);
	}
	template<> std::unique_ptr<GenericRaster> as(SEXP sexp) {
		Profiler::Profiler p("R_OPERATOR: unwrapping raster");
		***REMOVED***::S4 rasterlayer(sexp);
		if (!rasterlayer.is("RasterLayer"))
			throw OperatorException("R: Result is not a RasterLayer");

		int width = rasterlayer.slot("ncols");
		int height = rasterlayer.slot("nrows");

		***REMOVED***::S4 crs = rasterlayer.slot("crs");
		std::string epsg_string = crs.slot("projargs");
		epsg_t epsg = EPSG_UNKNOWN;
		if (epsg_string.compare(0,5,"EPSG:") == 0)
			epsg = std::stoi(epsg_string.substr(5, std::string::npos));
		if (epsg == EPSG_UNKNOWN)
			throw OperatorException("R: result raster has no projection of form EPSG:1234 set");

		***REMOVED***::S4 extent = rasterlayer.slot("extent");
		double xmin = extent.slot("xmin"), ymin = extent.slot("ymin"), xmax = extent.slot("xmax"), ymax = extent.slot("ymax");

		LocalCRS lcrs(epsg, width, height, xmin, ymin, (xmax-xmin)/width, (ymax-ymin)/height);

		***REMOVED***::S4 data = rasterlayer.slot("data");
		if ((bool) data.slot("inmemory") != true)
			throw OperatorException("R: result raster not inmemory");
		if ((bool) data.slot("haveminmax") != true)
			throw OperatorException("R: result raster does not have min/max");

		double min = data.slot("min");
		double max = data.slot("max");

		DataDescription dd(GDT_Float32, min, max, true, NAN);

		lcrs.verify();
		dd.verify();
		auto raster_out = GenericRaster::create(lcrs, dd, GenericRaster::Representation::CPU);
		Raster2D<float> *raster2d = (Raster2D<float> *) raster_out.get();

		***REMOVED***::NumericVector pixels = data.slot("values");
		int pos = 0;
		for (int y=0;y<height;y++) {
			for (int x=0;x<width;x++) {
				float val = pixels[pos++];
				raster2d->set(x, y, val);
			}
		}
		return raster_out;
	}
}
