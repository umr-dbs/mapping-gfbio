
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
	private:
};

ROperator::ROperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}
ROperator::~ROperator() {
}
REGISTER_OPERATOR(ROperator, "r");


static std::mutex r_running_mutex;
static GenericOperator *r_running_operator = nullptr;
static std::unique_ptr<GenericRaster> query_raster_source(const QueryRectangle &rect) {
	auto raster = r_running_operator->getRaster(rect);
	raster->setRepresentation(GenericRaster::Representation::CPU);
	return raster;
}

static std::unique_ptr<GenericRaster> query_raster_source2(void *user_data, const QueryRectangle &rect) {
	GenericOperator *op = (GenericOperator *) user_data;
	auto raster = op->getRaster(rect);
	raster->setRepresentation(GenericRaster::Representation::CPU);
	return raster;
}

static std::unique_ptr<GenericRaster> query_raster_source3(GenericOperator *op, int childidx, const QueryRectangle &rect) {
	auto raster = op->getRasterFromSource(childidx, rect);
	raster->setRepresentation(GenericRaster::Representation::CPU);
	return raster;
}


static void test_func(int bind, int value) {
	return;
}

std::unique_ptr<GenericRaster> ROperator::getRaster(const QueryRectangle &rect) {
	std::lock_guard<std::mutex> guard(r_running_mutex);

	***REMOVED*** R;

	//R.parseEvalQ("library(\"sp\");");
	//R.parseEvalQ("library(\"raster\")");

#if 0
	r_running_operator = sources[0];
	R["mapping.source"] = ***REMOVED***::InternalFunction( &query_raster_source );
#elif true
	std::function<std::unique_ptr<GenericRaster>(const QueryRectangle &rect)> bound_source = std::bind(query_raster_source3, this, 0, std::placeholders::_1);
	R["mapping.source"] = ***REMOVED***::InternalFunction( bound_source );
	//R["mapping.source"] = bound_source;

	std::function<void(int)> bound_test = std::bind(test_func, 42, std::placeholders::_1);
	R["test_func"] = ***REMOVED***::InternalFunction( bound_test );

	std::pair<double, int> pair;
	R["pair"] = pair;
	//R["test_func"] = bound_test;
	//R["test_func2"] = &test_func;
#endif
	R["mapping.qrect"] = rect;

	auto result = R.parseEval(
//		"test_func(1);"
//		"test_func2(1,2);"
		"raster = mapping.source(mapping.qrect);"
		"raster <- -raster;"
		"raster;"
	);

	std::unique_ptr<GenericRaster> raster_out_guard = result;

	// TODO: reuse crs from QueryRectangle?
	// We're violating encapsulation here, just to
	LocalCRS qrect_crs(rect);
	LocalCRS &raster_crs = const_cast<LocalCRS &>(raster_out_guard->lcrs);
	raster_crs.epsg = qrect_crs.epsg;
	for (int i=0;i<3;i++) {
		raster_crs.origin[i] = qrect_crs.origin[i];
		raster_crs.scale[i] = qrect_crs.scale[i];
	}
	raster_crs.scale[1] *= -1;
	raster_crs.verify();



	return raster_out_guard;
}




namespace ***REMOVED*** {
	// QueryRectangle
	template<> SEXP wrap(const QueryRectangle &rect) {
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
		int width = raster.lcrs.size[0];
		int height = raster.lcrs.size[1];

		/*
		 * Thomas sagt: "Raster" und "Spatial Data Frame"?
		 */

		/*
		 * Spatial Grid
		 * grid: GridTopology
		 * bbox: Matrix, bounding box
		 * proj4string: CRS
		 *
		 * GridTopology:
		 * cellcentre.offset: vector, origin
		 * cellsize: vector, scale
		 * cells.dim: vector, size
		 *
		 * CRS:
		 * projargs: string
		 */

		// TODO: use IntegerMatrix for integer datatypes?
		***REMOVED***::NumericMatrix M(width, height);
		for (int y=0;y<height;y++)
			for (int x=0;x<width;x++) {
				double val = raster.getAsDouble(x, y);
				if (raster.dd.is_no_data(val))
					M(x,y) = NAN;
				else
					M(x,y) = val;
			}
		return ***REMOVED***::wrap(M);
	}
	template<> SEXP wrap(const std::unique_ptr<GenericRaster> &raster) {
		return ***REMOVED***::wrap(*raster);
	}
	template<> std::unique_ptr<GenericRaster> as(SEXP sexp) {
		***REMOVED***::NumericMatrix M = ***REMOVED***::as<***REMOVED***::NumericMatrix>(sexp);

		int width = M.nrow(); // In R Matrixes, the first dimension is "rows"..
		int height = M.ncol();

		// TODO: the R raster must have a CRS
		LocalCRS lcrs(EPSG_UNKNOWN, width, height, 0, 0, 1, 1);
		// TODO: the R raster must have min/max values
		DataDescription dd(GDT_Float32, -1024, 1024);

		auto raster_out_guard = GenericRaster::create(lcrs, dd);
		Raster2D<float> *raster_out = (Raster2D<float> *) raster_out_guard.get();

	    for (int y=0;y<height;y++)
	    	for (int x=0;x<width;x++)
	    		raster_out->set(x, y, (float) M(x,y));

		return raster_out_guard;
	}
}
