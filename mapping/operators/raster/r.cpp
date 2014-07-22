
#include "raster/raster.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "operators/operator.h"


#include <limits>
#include <memory>
#include <sstream>
#include <json/json.h>

#include <***REMOVED***Common.h>

/*
namespace ***REMOVED*** {
	template<typename T> SEXP wrap(const Raster2D<T> &raster) {
		int width = raster->lcrs.size[0];
		int height = raster->lcrs.size[1];

	    ***REMOVED***::NumericMatrix M(width, height);
	    for (int y=0;y<height;y++)
	    	for (int x=0;x<width;x++)
	    		M(x,y) = (double) raster->get(x, y);
		return M;
	}
	template<> Raster2D<float> *as(SEXP sexp) {
		// TODO
		return nullptr;
	}
}
*/

#include <***REMOVED***.h>

class ROperator : public GenericOperator {
	public:
	ROperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~ROperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect);
	private:
};



ROperator::ROperator(int sourcecount, GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(1);
}
ROperator::~ROperator() {
}
REGISTER_OPERATOR(ROperator, "r");

/*
static double testfunc( double x ) {
    return 42.0 + x;
}

***REMOVED***Export SEXP testfunc2( SEXP x ) {
	return ***REMOVED***::wrap(42.0);
}

SEXP testfunc(SEXP x) {
	return ***REMOVED***::wrap(42.0);
}
*/

std::unique_ptr<GenericRaster> ROperator::getRaster(const QueryRectangle &rect) {
	auto raster_in = sources[0]->getRaster(rect);
	raster_in->setRepresentation(GenericRaster::Representation::CPU);

	***REMOVED*** R;

	//***REMOVED***::List mapping;
	//R["mapping"] = mapping;

	//R["testfunc"] = ***REMOVED***::InternalFunction( &testfunc );
	//R["testfunc2"] = testfunc2;
	/*
	double result = R.parseEval("testfunc2(0.0)");
	if (result != 42.0)
		throw OperatorException("result != 42!");
	*/

	auto pixels = raster_in->lcrs.getPixelCount();
	int width = raster_in->lcrs.size[0];
	int height = raster_in->lcrs.size[1];

	// TODO: use IntegerMatrix for integer datatypes?
    ***REMOVED***::NumericMatrix M_in(width, height);
    for (int y=0;y<height;y++)
    	for (int x=0;x<width;x++)
    		M_in(x,y) = raster_in->getAsDouble(x, y);

	R["raster"] = M_in;

	R.parseEvalQ("raster <- -raster");

	***REMOVED***::NumericMatrix M_out = R["raster"];


	DataDescription out_dd = raster_in->dd;
	out_dd.datatype = GDT_Float32;

	auto raster_out_guard = GenericRaster::create(raster_in->lcrs, out_dd);
	Raster2D<float> *raster_out = (Raster2D<float> *) raster_out_guard.get();

    for (int y=0;y<height;y++)
    	for (int x=0;x<width;x++)
    		raster_out->set(x, y, (float) M_out(x,y));

	return raster_out_guard;
}
