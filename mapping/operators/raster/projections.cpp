
#include "raster/raster.h"
#include "raster/pointcollection.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "operators/operator.h"
#include "util/gdal.h"

#include <memory>
#include <sstream>
#include <cmath>
#include <json/json.h>


class ProjectionOperator : public GenericOperator {
	public:
		ProjectionOperator(int sourcecount, GenericOperator *sources[], Json::Value params);
		virtual ~ProjectionOperator();

		virtual GenericRaster *getRaster(const QueryRectangle &rect);
		virtual PointCollection *getPoints(const QueryRectangle &rect);
	private:
		QueryRectangle projectQueryRectangle(const QueryRectangle &rect, const GDAL::CRSTransformer &transformer);
		epsg_t src_epsg, dest_epsg;
};


#if 0
class MeteosatLatLongOperator : public GenericOperator {
	public:
	MeteosatLatLongOperator(int sourcecount, GenericOperator *sources[]);
		virtual ~MeteosatLatLongOperator();

		virtual GenericRaster *getRaster(const QueryRectangle &rect);
};
#endif



ProjectionOperator::ProjectionOperator(int sourcecount, GenericOperator *sources[], Json::Value params) : GenericOperator(Type::RASTER, sourcecount, sources) {
	src_epsg = params.get("src_epsg", EPSG_UNKNOWN).asInt();
	dest_epsg = params.get("dest_epsg", EPSG_UNKNOWN).asInt();
	if (src_epsg == EPSG_UNKNOWN || dest_epsg == EPSG_UNKNOWN)
		throw OperatorException("Unknown EPSG");
	assumeSources(1);
}

ProjectionOperator::~ProjectionOperator() {
}
REGISTER_OPERATOR(ProjectionOperator, "projection");

template<typename T>
struct raster_projection {
	static GenericRaster *execute(Raster2D<T> *raster_src, const GDAL::CRSTransformer *transformer, LocalCRS &rm_dest ) {
		raster_src->setRepresentation(GenericRaster::Representation::CPU);

		DataDescription out_dd = raster_src->dd;
		out_dd.addNoData();

		Raster2D<T> *raster_dest = (Raster2D<T> *) GenericRaster::create(rm_dest, out_dd);
		std::unique_ptr<GenericRaster> raster_dest_guard(raster_dest);

		T nodata = (T) out_dd.no_data;

		int width = raster_dest->lcrs.size[0];
		int height = raster_dest->lcrs.size[1];
		for (int y=0;y<height;y++) {
			for (int x=0;x<width;x++) {
				double px = raster_dest->lcrs.PixelToWorldX(x);
				double py = raster_dest->lcrs.PixelToWorldY(y);
				double pz = 0;

				if (!transformer->transform(px, py, pz)) {
					raster_dest->set(x, y, nodata);
					continue;
				}

				int tx = round(raster_src->lcrs.WorldToPixelX(px));
				int ty = round(raster_src->lcrs.WorldToPixelY(py));
				if (tx >= 0 && ty >= 0 && tx < (int) raster_src->lcrs.size[0] && ty < (int) raster_src->lcrs.size[1]) {
					raster_dest->set(x, y, raster_src->get(tx, ty));
				}
				else
					raster_dest->set(x, y, nodata);
			}
		}

		return raster_dest_guard.release();
	}
};


QueryRectangle ProjectionOperator::projectQueryRectangle(const QueryRectangle &rect, const GDAL::CRSTransformer &transformer) {
	double src_x1, src_y1, src_x2, src_y2;
	int src_xres = rect.xres, src_yres = rect.yres;

	if (src_epsg == EPSG_METEOSAT2) {
		// Meteosat: ALWAYS load the full raster.
		src_x1 = 0;
		src_y1 = 0;
		src_x2 = 3711;
		src_y2 = 3711;
		src_xres = 3712;
		src_yres = 3712;
	}
	else {
		// Transform the upper left and bottom right corner, use those as the source bounding box
		// That'll only work on transformations where rectangles remain rectangles..
		double px = rect.x1, py = rect.y1, pz=0;
		if (!transformer.transform(px, py, pz))
			throw OperatorException("Transformation of top left corner failed");
		src_x1 = px;
		src_y1 = py;

		px = rect.x2;
		py = rect.y2;
		if (!transformer.transform(px, py, pz))
			throw OperatorException("Transformation of bottom right corner failed");
		src_x2 = px;
		src_y2 = py;

		// TODO: welche Auflösung der Quelle brauchen wir denn überhaupt?
/*
		printf("Content-type: text/plain\r\n\r\n");
		printf("qrect: %f, %f -> %f, %f\n", rect.x1, rect.y1, rect.x2, rect.y2);
		printf("src:   %f, %f -> %f, %f\n", src_x1, src_y1, src_x2, src_y2);
		exit(0);
*/
	}

	return QueryRectangle(rect.timestamp, src_x1, src_y1, src_x2, src_y2, src_xres, src_yres, src_epsg);
}

//GenericRaster *ProjectionOperator::execute(int timestamp, double x1, double y1, double x2, double y2, int xres, int yres) {
GenericRaster *ProjectionOperator::getRaster(const QueryRectangle &rect) {
	if (dest_epsg != rect.epsg) {
		throw OperatorException("Projection: asked to transform to a different CRS than specified in QueryRectangle");
	}
	if (src_epsg == dest_epsg) {
		return sources[0]->getRaster(rect);
	}

	if (dest_epsg == EPSG_METEOSAT2)
		throw OperatorException("Cannot transform raster to Meteosat Projection. Why would you want that?");

	GDAL::CRSTransformer transformer(dest_epsg, src_epsg);

	QueryRectangle src_rect = projectQueryRectangle(rect, transformer);

	GenericRaster *raster = sources[0]->getRaster(src_rect);
	std::unique_ptr<GenericRaster> raster_guard(raster);

	if (src_epsg != raster->lcrs.epsg)
		throw OperatorException("ProjectionOperator: Source Raster not in expected projection");

	LocalCRS rm_dest(rect);

	Profiler::Profiler p("PROJECTION_OPERATOR");
	GenericRaster *result_raster = nullptr;
	result_raster = callUnaryOperatorFunc<raster_projection>(raster, &transformer, rm_dest);

	return result_raster;
}


PointCollection *ProjectionOperator::getPoints(const QueryRectangle &rect) {
	if (dest_epsg != rect.epsg)
		throw OperatorException("Projection: asked to transform to a different CRS than specified in QueryRectangle");
	if (src_epsg == dest_epsg)
		return sources[0]->getPoints(rect);

	if (src_epsg == EPSG_METEOSAT2 || dest_epsg == EPSG_METEOSAT2)
		throw OperatorException("Cannot transform Points from or to Meteosat Projection. Why would you want that?");


	// Need to transform "backwards" to project the query rectangle..
	GDAL::CRSTransformer qrect_transformer(dest_epsg, src_epsg);
	QueryRectangle src_rect = projectQueryRectangle(rect, qrect_transformer);

	// ..but "forward" to project the points
	GDAL::CRSTransformer transformer(src_epsg, dest_epsg);


	PointCollection *points_in = sources[0]->getPoints(src_rect);
	std::unique_ptr<PointCollection> points_in_guard(points_in);

	if (src_epsg != points_in->epsg)
		throw OperatorException("ProjectionOperator: Source Points not in expected projection");

	PointCollection *points_out = new PointCollection(dest_epsg);
	std::unique_ptr<PointCollection> points_out_guard(points_out);

	// TODO: copy global metadata
	// TODO: copy local metadata indexes

	//printf("content-type: text/plain\r\n\r\n");
	for (Point &point : points_in->collection) {
		double x = point.x, y = point.y;
		if (!transformer.transform(x, y)) {
			continue;
		}

		//printf("%f, %f -> %f, %f\n", point.x, point.y, x, y);
		Point &p = points_out->addPoint(x, y);
		// TODO: copy local metadata
	}

	return points_out_guard.release();
}





#if 0
template<typename T>
struct meteosat_draw_latlong{
	static GenericRaster *execute(Raster2D<T> *raster_src) {
		std::unique_ptr<GenericRaster> raster_src_guard(raster_src);

		if (raster_src->lcrs.epsg != EPSG_METEOSAT2)
			throw OperatorException("Source raster not in meteosat projection");

		raster_src->setRepresentation(GenericRaster::Representation::CPU);

		T max = raster_src->dd.max*2;
		DataDescription vm_dest(raster_src->dd.datatype, raster_src->dd.min, max, raster_src->dd.has_no_data, raster_src->dd.no_data);
		Raster2D<T> *raster_dest = (Raster2D<T> *) GenericRaster::create(raster_src->lcrs, vm_dest);
		std::unique_ptr<GenericRaster> raster_dest_guard(raster_dest);

		// erstmal alles kopieren
		int width = raster_dest->lcrs.size[0];
		int height = raster_dest->lcrs.size[1];
		for (int y=0;y<height;y++) {
			for (int x=0;x<width;x++) {
				raster_dest->set(x, y, raster_src->get(x, y));
			}
		}

		// LATLONG zeichnen
		for (float lon=-180;lon<=180;lon+=10) {
			for (float lat=-90;lat<=90;lat+=0.01) {
				int px, py;
				GDAL::CRSTransformer::msg_geocoord2pixcoord(lon, lat, &px, &py);
				if (px < 0 || py < 0)
					continue;
				raster_dest->set(px, py, max);
			}
		}


		for (float lon=-180;lon<=180;lon+=0.01) {
			for (float lat=-90;lat<=90;lat+=10) {
				int px, py;
				GDAL::CRSTransformer::msg_geocoord2pixcoord(lon, lat, &px, &py);
				if (px < 0 || py < 0)
					continue;
				raster_dest->set(px, py, max);
			}
		}

		return raster_dest_guard.release();
	}
};


MeteosatLatLongOperator::MeteosatLatLongOperator(int sourcecount, GenericOperator *sources[]) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(1);
}
MeteosatLatLongOperator::~MeteosatLatLongOperator() {
}

GenericRaster *MeteosatLatLongOperator::getRaster(const QueryRectangle &rect) {

	GenericRaster *raster = sources[0]->getRaster(rect);

	Profiler::Profiler p("METEOSAT_DRAW_LATLONG_OPERATOR");
	return callUnaryOperatorFunc<meteosat_draw_latlong>(raster);
}

#endif


