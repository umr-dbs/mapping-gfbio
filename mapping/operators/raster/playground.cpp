
#include "raster/raster.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "operators/operator.h"

#include <memory>
#include <cmath>
#include <json/json.h>


class NegateOperator : public GenericOperator {
	public:
		NegateOperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~NegateOperator();

		virtual GenericRaster *getRaster(const QueryRectangle &rect);
};


class AddOperator : public GenericOperator {
	public:
		AddOperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~AddOperator();

		virtual GenericRaster *getRaster(const QueryRectangle &rect);
};



NegateOperator::NegateOperator(int sourcecount, GenericOperator *sources[], Json::Value &) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(1);
}
NegateOperator::~NegateOperator() {
}
REGISTER_OPERATOR(NegateOperator, "negate");

template<typename T>
struct negate{
	static GenericRaster *execute(Raster2D<T> *raster) {
		raster->setRepresentation(GenericRaster::Representation::CPU);

		T max = (T) raster->valuemeta.max;
		T min = (T) raster->valuemeta.min;
		T nodata = (T) raster->valuemeta.no_data;
#if 1
		int size = raster->rastermeta.getPixelCount();
		if (raster->valuemeta.has_no_data) {
			for (int i=0;i<size;i++) {
				T d = raster->data[i];
				// naja, ist auch nicht korrekt, aber egal
				if (d != nodata) {
					d = max - (d - min);
					if (d == nodata)
						d = nodata + 1;
				}
				raster->data[i] = d;
			}
		}
		else {
			for (int i=0;i<size;i++) {
				raster->data[i] = max - (raster->data[i] - min);
			}
		}
#else
		int width = raster->rastermeta.size[0];
		int height = raster->rastermeta.size[1];
		for (int y=0;y<height;y++) {
			for (int x=0;x<width;x++) {
				raster->set(x, y, max - (raster->get(x, y) - min));
			}
		}
#endif
		return raster;
	}
};


GenericRaster *NegateOperator::getRaster(const QueryRectangle &rect) {
	GenericRaster *raster = sources[0]->getRaster(rect);

	Profiler::Profiler p("NEGATE_OPERATOR");
	return callUnaryOperatorFunc<negate>(raster);
}






// AddOperator
AddOperator::AddOperator(int sourcecount, GenericOperator *sources[], Json::Value &) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(2);
}

AddOperator::~AddOperator() {
}
REGISTER_OPERATOR(AddOperator, "add");

template<typename T1, typename T2>
struct add{
	static GenericRaster *execute(Raster2D<T1> *raster1, Raster2D<T2> *raster2) {
		std::unique_ptr<GenericRaster> raster1_guard(raster1);
		std::unique_ptr<GenericRaster> raster2_guard(raster2);

		// nur ein Beispiel, nach der addition wird innerhalb des Wertebereichs normalisiert.
		T1 min = (T1) raster1->valuemeta.min;
		int size = raster1->rastermeta.getPixelCount();
		for (int i=0;i<size;i++) {
			raster1->data[i] = ((raster1->data[i] - min) + (raster2->data[i] - min))/2 + min;
		}

		return raster1_guard.release();
	};
};


GenericRaster *AddOperator::getRaster(const QueryRectangle &rect) {
	std::unique_ptr<GenericRaster> raster1( sources[0]->getRaster(rect) );
	std::unique_ptr<GenericRaster> raster2( sources[1]->getRaster(rect) );

	if (!(raster1->rastermeta == raster2->rastermeta))
		throw OperatorException("add: rasters differ in rastermeta");
	if (!(raster1->valuemeta == raster2->valuemeta))
		throw OperatorException("add: rasters differ in valuemeta");

	raster1->setRepresentation(GenericRaster::Representation::CPU);
	raster2->setRepresentation(GenericRaster::Representation::CPU);

	Profiler::Profiler p("ADD_OPERATOR");
	return callBinaryOperatorFunc<add>(raster1.release(), raster2.release());
}

