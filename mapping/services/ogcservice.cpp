
#include "services/ogcservice.h"
#include "datatypes/colorizer.h"
#include "util/timeparser.h"
#include "util/exceptions.h"


epsg_t OGCService::parseEPSG(const Params &params, const std::string &key, epsg_t defaultValue) {
	if (!params.hasParam(key))
		return defaultValue;
	return epsgCodeFromSrsString(params.get(key));
}

double OGCService::parseTimestamp(const Params &params, double defaultValue) {
	double timestamp = defaultValue;
	if (params.hasParam("timestamp")) {
		timestamp = TimeParser::create(TimeParser::Format::SECONDS)->parse(params.get("timestamp"));
	}
	if (params.hasParam("time")) { //TODO: prefer time over timestamp?
		timestamp = TimeParser::create(TimeParser::Format::ISO)->parse(params.get("time"));
	}
	return timestamp;
}

void OGCService::parseBBOX(double *bbox, const std::string bbox_str, epsg_t epsg, bool allow_infinite) {
	// &BBOX=0,0,10018754.171394622,10018754.171394622
	for(int i=0;i<4;i++)
		bbox[i] = NAN;

	auto extent = SpatialReference::extent(epsg);

	std::string delimiters = " ,";
	size_t current, next = -1;
	int element = 0;
	do {
		current = next + 1;
		next = bbox_str.find_first_of(delimiters, current);
		std::string stringValue = bbox_str.substr(current, next - current);
		double value = 0;

		if (stringValue == "Infinity") {
			if (!allow_infinite)
				throw ArgumentException("cannot process BBOX with Infinity");
			if (element == 0 || element == 2)
				value = std::max(extent.x1, extent.x2);
			else
				value = std::max(extent.y1, extent.y2);
		}
		else if (stringValue == "-Infinity") {
			if (!allow_infinite)
				throw ArgumentException("cannot process BBOX with Infinity");
			if (element == 0 || element == 2)
				value = std::min(extent.x1, extent.x2);
			else
				value = std::min(extent.y1, extent.y2);
		}
		else {
			value = std::stod(stringValue);
		}

		if (!std::isfinite(value))
			throw ArgumentException("BBOX contains entry that is not a finite number");

		bbox[element++] = value;
	} while (element < 4 && next != std::string::npos);

	if (element != 4)
		throw ArgumentException("Could not parse BBOX parameter");

	/*
	 * OpenLayers insists on sending latitude in x and longitude in y.
	 * The MAPPING code (including gdal's projection classes) don't agree: east/west should be in x.
	 * The simple solution is to swap the x and y coordinates.
	 * OpenLayers 3 uses the axis orientation of the projection to determine the bbox axis order. https://github.com/openlayers/ol3/blob/master/src/ol/source/imagewmssource.js ~ line 317.
	 */
	if (epsg == EPSG_LATLON) {
		std::swap(bbox[0], bbox[1]);
		std::swap(bbox[2], bbox[3]);
	}

	// If no extent is known, just trust the client.
	if (std::isfinite(extent.x1)) {
		double bbox_normalized[4];
		for (int i=0;i<4;i+=2) {
			bbox_normalized[i  ] = (bbox[i  ] - extent.x1) / (extent.x2-extent.x1);
			bbox_normalized[i+1] = (bbox[i+1] - extent.y1) / (extent.y2-extent.y1);
		}

		// Koordinaten können leicht ausserhalb liegen, z.B.
		// 20037508.342789, 20037508.342789
		for (int i=0;i<4;i++) {
			if (bbox_normalized[i] < 0.0 && bbox_normalized[i] > -0.001)
				bbox_normalized[i] = 0.0;
			else if (bbox_normalized[i] > 1.0 && bbox_normalized[i] < 1.001)
				bbox_normalized[i] = 1.0;
		}

		for (int i=0;i<4;i++) {
			if (bbox_normalized[i] < 0.0 || bbox_normalized[i] > 1.0)
				throw ArgumentException("BBOX exceeds extent");
		}
	}

	//bbox_normalized[1] = 1.0 - bbox_normalized[1];
	//bbox_normalized[3] = 1.0 - bbox_normalized[3];
}





void OGCService::outputImage(HTTPResponseStream &stream, GenericRaster *raster, bool flipx, bool flipy, const std::string &colors, Raster2D<uint8_t> *overlay) {
	// For now, always guess the colorizer, ignore any user-specified colors
	//auto colorizer = Colorizer::create(colors);
	auto colorizer = Colorizer::fromUnit(raster->dd.unit);

	if (!stream.hasSentHeaders()) {
		stream.sendDebugHeader();
		stream.sendContentType("image/png");
		stream.finishHeaders();
	}

	raster->toPNG(stream, *colorizer, flipx, flipy, overlay); //"/tmp/xyz.tmp.png");
}

void OGCService::outputSimpleFeatureCollectionGeoJSON(HTTPResponseStream &stream, SimpleFeatureCollection *collection, bool displayMetadata) {
	stream.sendDebugHeader();
	stream.sendContentType("application/json");
	stream.finishHeaders();
	stream << collection->toGeoJSON(displayMetadata);
}

void OGCService::outputSimpleFeatureCollectionCSV(HTTPResponseStream &stream, SimpleFeatureCollection *collection) {
	stream.sendDebugHeader();
	stream.sendContentType("text/csv");
	stream.sendHeader("Content-Disposition", "attachment; filename=\"export.csv\"");
	stream.finishHeaders();
	stream << collection->toCSV();
}

void OGCService::outputSimpleFeatureCollectionARFF(HTTPResponseStream &stream, SimpleFeatureCollection* collection){
	stream.sendDebugHeader();
	stream.sendContentType("text/json");
	stream.sendHeader("Content-Disposition", "attachment; filename=\"export.arff\"");
	stream.finishHeaders();
	stream << collection->toARFF();
}
