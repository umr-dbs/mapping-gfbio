
#include "datatypes/raster.h"
#include "datatypes/raster/typejuggling.h"
#include "datatypes/simplefeaturecollections/geosgeomutil.h"
#include "operators/operator.h"
#include "util/gdal.h"
#include "util/make_unique.h"

#include <memory>
#include <sstream>
#include <cmath>
#include <vector>
#include <json/json.h>
#include <geos/geom/util/GeometryTransformer.h>
#include "datatypes/pointcollection.h"
#include "datatypes/polygoncollection.h"

class ProjectionOperator : public GenericOperator {
	public:
		ProjectionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~ProjectionOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<LineCollection> getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream &stream);
	private:
		QueryRectangle projectQueryRectangle(const QueryRectangle &rect, const GDAL::CRSTransformer &transformer);
		epsg_t src_epsg, dest_epsg;
};


#if 0
class MeteosatLatLongOperator : public GenericOperator {
	public:
	MeteosatLatLongOperator(int sourcecount, GenericOperator *sources[]);
		virtual ~MeteosatLatLongOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
};
#endif



ProjectionOperator::ProjectionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	src_epsg = epsgCodeFromSrsString(params["src_projection"].asString(), EPSG_UNKNOWN);
	dest_epsg = epsgCodeFromSrsString(params["dest_projection"].asString(), EPSG_UNKNOWN);
	if (src_epsg == EPSG_UNKNOWN || dest_epsg == EPSG_UNKNOWN)
		throw OperatorException("Unknown EPSG");
	assumeSources(1);
}

ProjectionOperator::~ProjectionOperator() {
}
REGISTER_OPERATOR(ProjectionOperator, "projection");

void ProjectionOperator::writeSemanticParameters(std::ostringstream &stream) {
	stream << "\"src_projection\": \"EPSG:" << (int) src_epsg << "\", \"dest_projection\": \"EPSG:" << (int) dest_epsg << "\"";
}

#ifndef MAPPING_OPERATOR_STUBS
template<typename T>
struct raster_projection {
	static std::unique_ptr<GenericRaster> execute(Raster2D<T> *raster_src, const GDAL::CRSTransformer *transformer, const SpatioTemporalReference &stref_dest, uint32_t width, uint32_t height) {
		raster_src->setRepresentation(GenericRaster::Representation::CPU);

		DataDescription out_dd = raster_src->dd;
		out_dd.addNoData();

		auto raster_dest_guard = GenericRaster::create(raster_src->dd, stref_dest, width, height);
		Raster2D<T> *raster_dest = (Raster2D<T> *) raster_dest_guard.get();

		T nodata = (T) out_dd.no_data;

		for (uint32_t y=0;y<raster_dest->height;y++) {
			for (uint32_t x=0;x<raster_dest->width;x++) {
				double px = raster_dest->PixelToWorldX(x);
				double py = raster_dest->PixelToWorldY(y);
				double pz = 0;

				if (!transformer->transform(px, py, pz)) {
					raster_dest->set(x, y, nodata);
					continue;
				}

				auto tx = raster_src->WorldToPixelX(px);
				auto ty = raster_src->WorldToPixelY(py);
				raster_dest->set(x, y, raster_src->getSafe(tx, ty, nodata));
			}
		}

		return raster_dest_guard;
	}
};


QueryRectangle ProjectionOperator::projectQueryRectangle(const QueryRectangle &rect, const GDAL::CRSTransformer &transformer) {
	double src_x1, src_y1, src_x2, src_y2;
	int src_xres = rect.xres, src_yres = rect.yres;

	const double MSG_MAX_LAT = 79.0;  // north/south
	const double MSG_MAX_LONG = 76.0; // east/west

	if (dest_epsg == EPSG_GEOSMSG) {
		// We're loading some points and would like to display them in the msg projection. Why? Well, why not?
		if (src_epsg == EPSG_WEBMERCATOR) {
			// TODO: this is the whole world. A smaller rectangle would do, we just need to figure out the coordinates.
			src_x1 = -20037508.34;
			src_y1 = -20037508.34;
			src_x2 = 20037508.34;
			src_y2 = 20037508.34;
		}
		else if (src_epsg == EPSG_LATLON) {
			src_x1 = -MSG_MAX_LONG;
			src_y1 = -MSG_MAX_LAT;
			src_x2 = MSG_MAX_LONG;
			src_y2 = MSG_MAX_LAT;
		}
		else
			throw OperatorException("Cannot transform to METEOSAT2 projection from this projection");
	}
	else if (src_epsg == EPSG_GEOSMSG) {
		/*
		 * We're loading a msg raster. Since a rectangle in latlon or mercator does not map to
		 * an exact rectangle in MSG, we need to use some heuristics
		 */
		double tlx = rect.x1, tly = rect.y1, brx = rect.x2, bry = rect.y2;

		if (dest_epsg != EPSG_LATLON) {
			GDAL::CRSTransformer transformer_tolatlon(dest_epsg, EPSG_LATLON);
			double pz=0;
			if (!transformer_tolatlon.transform(tlx, tly, pz))
				throw OperatorException("Transformation of top left corner failed");
			if (!transformer_tolatlon.transform(brx, bry, pz))
				throw OperatorException("Transformation of bottom right corner failed");
		}

		double top = std::max(tly, bry);
		double bottom = std::min(tly, bry);
		double left = std::min(tlx, brx);
		double right = std::max(tlx, brx);

		// First optimization: see if we're on a part of the earth visible by the satellite
		if (bottom > MSG_MAX_LAT || top < -MSG_MAX_LAT || right < -MSG_MAX_LONG || left > MSG_MAX_LONG) {
			/*
			std::ostringstream msg;
			msg << "Projection: there is no source data here (" << left << "," << top << ") -> (" << right << "," << bottom << ")";
			throw OperatorException(msg.str());
			*/

			// return a very small source rectangle with minimum resolution
			return QueryRectangle(
				SpatialReference(src_epsg, 0, 0, 1, 1),
				rect,
				rect.restype == QueryResolution::Type::PIXELS ? QueryResolution::pixels(1, 1) : QueryResolution(rect)
			);
		}

		// By default: pick the whole raster
		src_x1 = -5568748.276;
		src_y1 = -5568748.276;
		src_x2 = 5568748.276;
		src_y2 = 5568748.276;
		// Second optimization: see if we can restrict us to a quarter of the globe
		if (left > 0)
			src_x1 = 0;
		if (right < 0)
			src_x2 = 0;
		if (top < 0)
			src_y2 = 0;
		if (bottom > 0)
			src_y1 = 0;


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

	QueryRectangle result(
		SpatialReference(src_epsg, src_x1, src_y1, src_x2, src_y2),
		rect,
		rect.restype == QueryResolution::Type::PIXELS ? QueryResolution::pixels(src_xres, src_yres) : QueryResolution(rect)
	);
	if (result.restype == QueryResolution::Type::PIXELS)
		result.enlarge(2);
	return result;
}

//GenericRaster *ProjectionOperator::execute(int timestamp, double x1, double y1, double x2, double y2, int xres, int yres) {
std::unique_ptr<GenericRaster> ProjectionOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	if (dest_epsg != rect.epsg) {
		throw OperatorException("Projection: asked to transform to a different CRS than specified in QueryRectangle");
	}
	if (src_epsg == dest_epsg) {
		return getRasterFromSource(0, rect, profiler);
	}

	GDAL::CRSTransformer transformer(dest_epsg, src_epsg);

	QueryRectangle src_rect = projectQueryRectangle(rect, transformer);

	auto raster_in = getRasterFromSource(0, src_rect, profiler);

	if (src_epsg != raster_in->stref.epsg)
		throw OperatorException("ProjectionOperator: Source Raster not in expected projection");

	return callUnaryOperatorFunc<raster_projection>(raster_in.get(), &transformer, rect, rect.xres, rect.yres);
}


std::unique_ptr<PointCollection> ProjectionOperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	if (dest_epsg != rect.epsg)
		throw OperatorException("Projection: asked to transform to a different CRS than specified in QueryRectangle");
	if (src_epsg == dest_epsg)
		return getPointCollectionFromSource(0, rect, profiler);


	// Need to transform "backwards" to project the query rectangle..
	GDAL::CRSTransformer qrect_transformer(dest_epsg, src_epsg);
	QueryRectangle src_rect = projectQueryRectangle(rect, qrect_transformer);

	// ..but "forward" to project the points
	GDAL::CRSTransformer transformer(src_epsg, dest_epsg);


	auto points_in = getPointCollectionFromSource(0, src_rect, profiler);

	if (src_epsg != points_in->stref.epsg) {
		std::ostringstream msg;
		msg << "ProjectionOperator: Source Points not in expected projection, expected " << (int) src_epsg << " got " << (int) points_in->stref.epsg;
		throw OperatorException(msg.str());
	}

	std::vector<bool> keep(points_in->getFeatureCount(), true);
	bool has_filter = false;

	double minx = rect.minx(), maxx = rect.maxx(), miny = rect.miny(), maxy = rect.maxy();

	for(auto feature : *points_in){
		for(auto& coordinate : feature){
			double x = coordinate.x, y = coordinate.y;
			if (!transformer.transform(x, y) || x < minx || x > maxx || y < miny || y > maxy) {
				keep[feature] = false;
				has_filter = true;
				break;
			}
			else {
				coordinate.x = x;
				coordinate.y = y;
			}
		}
	}

	points_in->replaceSTRef(rect);

	if (!has_filter)
		return points_in;
	else
		return points_in->filter(keep);
}




std::unique_ptr<LineCollection> ProjectionOperator::getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	if (dest_epsg != rect.epsg)
		throw OperatorException("Projection: asked to transform to a different CRS than specified in QueryRectangle");

	GDAL::CRSTransformer qrect_transformer(dest_epsg, src_epsg);
	QueryRectangle src_rect = projectQueryRectangle(rect, qrect_transformer);

	GDAL::CRSTransformer transformer(src_epsg, dest_epsg);

	auto lines_in = getLineCollectionFromSource(0, src_rect, profiler);

	if (src_epsg != lines_in->stref.epsg) {
		std::ostringstream msg;
		msg << "ProjectionOperator: Source Lines not in expected projection, expected " << (int) src_epsg << " got " << (int) lines_in->stref.epsg;
		throw OperatorException(msg.str());
	}

	std::vector<bool> keep(lines_in->getFeatureCount(), true);
	bool has_filter = false;

	double minx = rect.minx(), maxx = rect.maxx(), miny = rect.miny(), maxy = rect.maxy();

	for(auto feature : *lines_in){
		for(auto line : feature){
			for(auto& coordinate : line){
				double x = coordinate.x, y = coordinate.y;
				if (!transformer.transform(x, y) || x < minx || x > maxx || y < miny || y > maxy) {
					keep[feature] = false;
					has_filter = true;
					break;
				}
				else {
					coordinate.x = x;
					coordinate.y = y;
				}
			}
			if(!keep[feature])
				break;
		}
	}

	lines_in->replaceSTRef(rect);

	if (!has_filter)
		return lines_in;
	else
		return lines_in->filter(keep);
}

//TODO: why is this in raster folder?
std::unique_ptr<PolygonCollection> ProjectionOperator::getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	if (dest_epsg != rect.epsg)
		throw OperatorException("Projection: asked to transform to a different CRS than specified in QueryRectangle");

	GDAL::CRSTransformer qrect_transformer(dest_epsg, src_epsg);
	QueryRectangle src_rect = projectQueryRectangle(rect, qrect_transformer);

	GDAL::CRSTransformer transformer(src_epsg, dest_epsg);


	auto polygons_in = getPolygonCollectionFromSource(0, src_rect, profiler);

	if (src_epsg != polygons_in->stref.epsg) {
		std::ostringstream msg;
		msg << "ProjectionOperator: Source Polygons not in expected projection, expected " << (int) src_epsg << " got " << (int) polygons_in->stref.epsg;
		throw OperatorException(msg.str());
	}

	std::vector<bool> keep(polygons_in->getFeatureCount(), true);
	bool has_filter = false;

	double minx = rect.minx(), maxx = rect.maxx(), miny = rect.miny(), maxy = rect.maxy();

	for(auto feature : *polygons_in){
		for(auto polygon : feature){
			for(auto ring : polygon){
				for(auto& coordinate : ring){
					double x = coordinate.x, y = coordinate.y;
					if (!transformer.transform(x, y) || x < minx || x > maxx || y < miny || y > maxy) {
						keep[feature] = false;
						has_filter = true;
						break;
					}
					else {
						coordinate.x = x;
						coordinate.y = y;
					}
				}
				if(!keep[feature])
					break;
			}
			if(!keep[feature])
				break;
		}
	}

	polygons_in->replaceSTRef(rect);

	if (!has_filter)
		return polygons_in;
	else
		return polygons_in->filter(keep);
}
#endif

#if 0
template<typename T>
struct meteosat_draw_latlong{
	static std::unique_ptr<GenericRaster> execute(Raster2D<T> *raster_src) {
		if (raster_src->lcrs.epsg != EPSG_METEOSAT2)
			throw OperatorException("Source raster not in meteosat projection");

		raster_src->setRepresentation(GenericRaster::Representation::CPU);

		T max = raster_src->dd.max*2;
		DataDescription vm_dest(raster_src->dd.datatype, raster_src->dd.min, max, raster_src->dd.has_no_data, raster_src->dd.no_data);
		auto raster_dest_guard = GenericRaster::create(raster_src->lcrs, vm_dest);
		Raster2D<T> *raster_dest = (Raster2D<T> *) raster_dest_guard.get();

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

		return raster_dest_guard;
	}
};


MeteosatLatLongOperator::MeteosatLatLongOperator(int sourcecount, GenericOperator *sources[]) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(1);
}
MeteosatLatLongOperator::~MeteosatLatLongOperator() {
}

std::unique_ptr<GenericRaster> MeteosatLatLongOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto raster_in = getRasterFromSource(0, rect, profiler);

	return callUnaryOperatorFunc<meteosat_draw_latlong>(raster_in.get());
}

#endif


