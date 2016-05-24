
#include "services/ogcservice.h"
#include "datatypes/colorizer.h"
#include "util/timeparser.h"
#include "util/exceptions.h"

#include <archive.h>
#include <archive_entry.h>
#include <cstring>
#include <vector>


epsg_t OGCService::parseEPSG(const Params &params, const std::string &key, epsg_t defaultValue) {
	if (!params.hasParam(key))
		return defaultValue;
	return epsgCodeFromSrsString(params.get(key));
}

TemporalReference OGCService::parseTime(const Params &parameters) const {
	TemporalReference tref(TIMETYPE_UNIX);
	if(parameters.hasParam("time")){
		//time is specified in ISO8601, it can either be an instant (single datetime) or an interval
		//An interval is separated by "/". "Either the start value or the end value can be omitted to
		//indicate no restriction on time in that direction."
		//sources: - http://docs.geoserver.org/2.8.x/en/user/services/wms/time.html#wms-time
		//         - http://www.ogcnetwork.net/node/178

		//TODO: relative intervals

		const std::string &timeString = parameters.get("time");
		auto timeParser = TimeParser::create(TimeParser::Format::ISO);
		size_t sep = timeString.find("/");

		if(sep == std::string::npos){
			//time is an instant
			tref.t1 = timeParser->parse(timeString);
			tref.t2 = tref.t1 + tref.epsilon();
		} else if (sep == 0){
			//time interval begins at begin of time
			tref.t2 = timeParser->parse(timeString.substr(1));
		}
		else {
			tref.t1 = timeParser->parse(timeString.substr(0, sep));
			if(sep < timeString.length() - 1) {
				tref.t2 = timeParser->parse(timeString.substr(sep + 1));
			}
			//else time interval ends at end of time
		}

		tref.validate();
	}
	return tref;
}

SpatialReference OGCService::parseBBOX(const std::string bbox_str, epsg_t epsg, bool allow_infinite) {
	auto extent = SpatialReference::extent(epsg);

	double bbox[4];
	for(int i=0;i<4;i++)
		bbox[i] = NAN;

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

	// If no extent is known, just trust the client and assume the bbox is within the extent.
	if (std::isfinite(extent.x1)) {
		double allowed_error_x = (extent.x2-extent.x1) / 1000;
		double allowed_error_y = (extent.y2-extent.y1) / 1000;

		// Koordinaten können leicht ausserhalb liegen, z.B.
		// 20037508.342789, 20037508.342789
		if ( (bbox[0] < extent.x1 - allowed_error_x)
		  || (bbox[1] < extent.y1 - allowed_error_y)
		  || (bbox[2] > extent.x2 + allowed_error_x)
		  || (bbox[3] > extent.y2 + allowed_error_y) ) {
			throw ArgumentException("BBOX exceeds extent");
		}
	}

	bool flipx, flipy;
	SpatialReference sref(epsg, bbox[0], bbox[1], bbox[2], bbox[3], flipx, flipy);
	return sref;
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

void OGCService::exportZip(const char* data, size_t dataLength, const std::string &format, ProvenanceCollection &provenance) {
	//data file name
	std::string fileExtension;
	if (format == "application/json")
		fileExtension = "json";
	else if (format == "csv")
		fileExtension = "csv";
	else
		throw ArgumentException("WFSService: unknown output format");
	std::string fileName = "data." + fileExtension;

	//archive creation
	struct archive *archive;
	struct archive_entry *entry;

	size_t bufferSize = dataLength * 2; //TODO determine reasonable size?
	std::vector<char> buffer(bufferSize);

	archive = archive_write_new();

	archive_write_set_format_zip(archive);
	size_t used;
	archive_write_open_memory(archive, buffer.data(), bufferSize, &used);

	//data
	entry = archive_entry_new();
	archive_entry_set_pathname(entry, fileName.c_str());
	archive_entry_set_size(entry, dataLength);
	archive_entry_set_filetype(entry, AE_IFREG);
	archive_entry_set_perm(entry, 0644);
	archive_write_header(archive, entry);

	archive_write_data(archive, data, dataLength);
	archive_entry_free(entry);

	//provenance info
	//TODO: format provenance info
	std::string json = provenance.toJson();
	entry = archive_entry_new();
	archive_entry_set_pathname(entry, "provenance.txt");
	archive_entry_set_size(entry, json.length());
	archive_entry_set_filetype(entry, AE_IFREG);
	archive_entry_set_perm(entry, 0644);
	archive_write_header(archive, entry);

	archive_write_data(archive, json.c_str(), json.length());
	archive_entry_free(entry);

	archive_write_close(archive);
	archive_write_free(archive);

	result.sendContentType(EXPORT_MIME_PREFIX + format);
	result.sendHeader("Content-Disposition", "attachment; filename=export.zip");
	result.sendHeader("Content-Length", concat(used));
	result.finishHeaders();
	result.write(reinterpret_cast<const char*>(buffer.data()), used);
}
