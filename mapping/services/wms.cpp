
#include "services/ogcservice.h"
#include "operators/operator.h"
#include "datatypes/raster.h"
#include "datatypes/raster/raster_priv.h"
#include "datatypes/plot.h"
#include "datatypes/colorizer.h"
#include "util/configuration.h"
#include "util/debug.h"


class WMSService : public OGCService {
	public:
		WMSService() = default;
		virtual ~WMSService() = default;
		virtual void run(const Params& params, HTTPResponseStream& result, std::ostream &error);
};
REGISTER_HTTP_SERVICE(WMSService, "WMS");


void WMSService::run(const Params& params, HTTPResponseStream& result, std::ostream &error) {
	bool debug = params.getBool("debug", Configuration::getBool("global.debug", false));
	auto query_epsg = parseEPSG(params, "crs");
	TemporalReference tref = parseTime(params);

	std::string request = params.get("request");
	// GetCapabilities
	if (request == "GetCapabilities") {
		// TODO!
	}
	// GetMap
	else if (request == "GetMap") {
		if (params.get("version") != "1.3.0")
			result.send500("Invalid version");

		int output_width = params.getInt("width");
		int output_height = params.getInt("height");
		if (output_width <= 0 || output_height <= 0) {
			result.send500("output_width not valid");
		}

		try {
			// Wir ignorieren:
			// format
			// transparent

			// Unbekannt:
			// &STYLES=dem

			//if (params["tiled"] != "true")
			//	send403("only tiled for now");

			SpatialReference sref = parseBBOX(params.get("bbox"), query_epsg, false);
			auto colorizer = params.get("colors", "");
			auto format = params.get("format", "image/png");


			bool flipx, flipy;
			QueryRectangle qrect(
				sref,
				tref,
				QueryResolution::pixels(output_width, output_height)
			);

			if (format == "application/json") {
				auto graph = GenericOperator::fromJSON(params.get("layers"));
				QueryProfiler profiler;
				std::unique_ptr<GenericPlot> dataVector = graph->getCachedPlot(qrect, profiler);

				result.sendContentType("application/json");
				result.finishHeaders();
				result << dataVector->toJSON();
				return;
			}
			else {
				double bbox[4] = {sref.x1, sref.y1, sref.x2, sref.y2};
				auto graph = GenericOperator::fromJSON(params.get("layers"));
				QueryProfiler profiler;
				auto result_raster = graph->getCachedRaster(qrect,profiler,GenericOperator::RasterQM::EXACT);
				bool flipx = (bbox[2] > bbox[0]) != (result_raster->pixel_scale_x > 0);
				bool flipy = (bbox[3] > bbox[1]) == (result_raster->pixel_scale_y > 0);

				std::unique_ptr<Raster2D<uint8_t>> overlay;
				if (debug) {
					Unit u = Unit::unknown();
					u.setMinMax(0, 1);
					DataDescription dd_overlay(GDT_Byte, u);
					overlay.reset( (Raster2D<uint8_t> *) GenericRaster::create(dd_overlay, SpatioTemporalReference::unreferenced(), output_width, output_height).release() );
					overlay->clear(0);

					// Write debug info
					std::ostringstream msg_tl;
					msg_tl.precision(2);
					msg_tl << std::fixed << bbox[0] << ", " << bbox[1] << " [" << result_raster->stref.x1 << ", " << result_raster->stref.y1 << "]";
					overlay->print(4, 4, 1, msg_tl.str().c_str());

					std::ostringstream msg_br;
					msg_br.precision(2);
					msg_br << std::fixed << bbox[2] << ", " << bbox[3] << " [" << result_raster->stref.x2 << ", " << result_raster->stref.y2 << "]";;
					std::string msg_brs = msg_br.str();
					overlay->print(overlay->width-4-8*msg_brs.length(), overlay->height-12, 1, msg_brs.c_str());

					if (result_raster->height >= 512) {
						auto messages = get_debug_messages();
						int ypos = 46;
						for (auto &msg : messages) {
							overlay->print(4, ypos, 1, msg.c_str());
							ypos += 10;
						}
						ypos += 20;
						overlay->print(4, ypos, 1, "Attributes:");
						ypos += 10;
						for (auto val : result_raster->global_attributes.numeric()) {
							std::ostringstream msg;
							msg << "attribute " << val.first << "=" << val.second;
							overlay->print(4, ypos, 1, msg.str().c_str());
							ypos += 10;
						}

					}
				}

				outputImage(result, result_raster.get(), flipx, flipy, colorizer, overlay.get());
			}
		}
		catch (const std::exception &e) {
			// Alright, something went wrong.
			// We're still in a WMS request though, so do our best to output an image with a clear error message.

			Unit u("errormessage", "errormessage");
			DataDescription dd(GDT_Byte, u, true, 0);
			auto errorraster = GenericRaster::create(dd, SpatioTemporalReference::unreferenced(), output_width, output_height);
			errorraster->clear(0);

			auto msg = e.what();
			errorraster->printCentered(1, msg);

			outputImage(result, errorraster.get(), false, false, "hsv");
		}
		// cut into pieces



		/*
	 	 	 ?SERVICE=WMS
	 	 	 &VERSION=1.3.0
	 	 	 &REQUEST=GetMap
	 	 	 &FORMAT=image%2Fpng8
	 	 	 &TRANSPARENT=true
	 	 	 &LAYERS=elevation%3Asrtm_41_90m
	 	 	 &TILED=true
	 	 	 &STYLES=dem
	 	 	 &WIDTH=256
	 	 	 &HEIGHT=256
	 	 	 &CRS=EPSG%3A3857
	 	 	 &BBOX=0%2C0%2C10018754.171394622%2C10018754.171394622
	 	 	*/
		/*
		addLayer(LayerType.WMS, {
			layer : {
				url : 'http://dbsvm.mathematik.uni-marburg.de:9833/geoserver/elevation/wms',
				params : {
					'LAYERS' : 'elevation:srtm_41_90m',
					'TILED' : true,
					'FORMAT' : 'image/png8',
					'STYLES' : 'dem'
				},
				serverType : 'geoserver'
			},
			title : "SRTM"
		});
		 */
	}
	else if (request == "GetColorizer") {
		if (params.get("version") != "1.3.0")
			result.send500("Invalid version");

		bool flipx, flipy;
		QueryRectangle qrect(
			SpatialReference::extent(query_epsg),
			tref,
			QueryResolution::pixels(1, 1)
		);

		auto graph = GenericOperator::fromJSON(params.get("layers"));
		QueryProfiler profiler;
		auto result_raster = graph->getCachedRaster(qrect,profiler,GenericOperator::RasterQM::LOOSE);

		auto unit = result_raster->dd.unit;
		auto colorizer = Colorizer::fromUnit(unit);

		result.sendContentType("application/json");
		result.finishHeaders();
		result << colorizer->toJson();
	}
	// GetFeatureInfo (optional)
	else if (request == "GetFeatureInfo") {
		result.send500("WMS::GetFeatureInfo not implemented");

	}
	else
		result.send500("unknown request");

}
