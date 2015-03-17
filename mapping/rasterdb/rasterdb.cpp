
#include "datatypes/raster.h"
#include "datatypes/raster/typejuggling.h"
#include "rasterdb/rasterdb.h"
#include "converters/converter.h"
#include "util/sqlite.h"
#include "operators/operator.h"


#include <unordered_map>
#include <mutex>
#include <limits.h>
#include <stdio.h>
#include <cstdlib>
#include <string.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#include <iostream>
#include <fstream>
#include <sstream>
#include <memory>
#include <string>

#include <json/json.h>


const int DEFAULT_TILE_SIZE = 2048;


bool GDALCRS::operator==(const GDALCRS &b) const {
	if (dimensions != b.dimensions)
		return false;
	for (int i=0;i<dimensions;i++) {
		if (size[i] != b.size[i]) {
			std::cerr << "size mismatch" << std::endl;
			return false;
		}
		if (fabs(origin[i] - b.origin[i]) > 0.5) {
			std::cerr << "origin mismatch: " << fabs(origin[i] - b.origin[i]) << std::endl;
			return false;
		}
		if (fabs(scale[i] / b.scale[i] - 1.0) > 0.001) {
			std::cerr << "scale mismatch" << std::endl;
			return false;
		}
	}
	return true;
}

void GDALCRS::verify() const {
	if (dimensions < 1 || dimensions > 3)
		throw MetadataException("Amount of dimensions not between 1 and 3");
	for (int i=0;i<dimensions;i++) {
		if (/*size[i] < 0 || */ size[i] > 1<<24)
			throw MetadataException("Size out of limits");
		if (scale[i] == 0)
			throw MetadataException("Scale cannot be 0");
	}
}

size_t GDALCRS::getPixelCount() const {
	if (dimensions == 1)
		return (size_t) size[0];
	if (dimensions == 2)
		return (size_t) size[0] * size[1];
	if (dimensions == 3)
		return (size_t) size[0] * size[1] * size[2];
	throw MetadataException("Amount of dimensions not between 1 and 3");
}

SpatioTemporalReference GDALCRS::toSpatioTemporalReference(bool &flipx, bool &flipy, timetype_t timetype, double t1, double t2) const {
	double x1 = origin[0];
	double y1 = origin[1];
	double x2 = origin[0] + scale[0] * size[0];
	double y2 = origin[1] + scale[1] * size[1];

	return SpatioTemporalReference(epsg, x1, y1, x2, y2, flipx, flipy, timetype, t1, t2);
}

std::ostream& operator<< (std::ostream &out, const GDALCRS &rm) {
	out << "GDALCRS(epsg=" << (int) rm.epsg << " dim=" << rm.dimensions << " size=["<<rm.size[0]<<","<<rm.size[1]<<"] origin=["<<rm.origin[0]<<","<<rm.origin[1]<<"] scale=["<<rm.scale[0]<<","<<rm.scale[1]<<"])";
	return out;
}



class RasterDBChannel {
	public:
		//friend class RasterDB;

		RasterDBChannel(const DataDescription &dd) : dd(dd), has_transform(false) {}
		~RasterDBChannel() {}

		void setTransform(GDALDataType datatype, double offset, double scale, const std::string &offset_metadata, const std::string &scale_metadata) {
			has_transform = true;
			transform_offset = offset;
			transform_scale = scale;
			transform_offset_metadata = offset_metadata;
			transform_scale_metadata = scale_metadata;
			transform_datatype = datatype == GDT_Unknown ? dd.datatype : datatype;
		}
		double getOffset(const DirectMetadata<double> &md) {
			if (!has_transform)
				return 0;
			if (transform_offset_metadata.length() > 0)
				return md.get(transform_offset_metadata, 0.0);
			return transform_offset;
		}
		double getScale(const DirectMetadata<double> &md) {
			if (!has_transform)
				return 0;
			if (transform_scale_metadata.length() > 0)
				return md.get(transform_scale_metadata, 1.0);
			return transform_scale;
		}
		DataDescription getTransformedDD(const DirectMetadata<double> &md) {
			if (!has_transform)
				return dd;
			double offset = getOffset(md);
			double scale = getScale(md);
			double transformed_min = dd.min * scale + offset;
			double transformed_max = dd.max * scale + offset;
			DataDescription transformed_dd(transform_datatype, transformed_min, transformed_max);
			transformed_dd.addNoData();
			transformed_dd.verify();
			return transformed_dd;
		}

		bool hasTransform() { return has_transform; }

		const DataDescription dd;
	private:
		bool has_transform;
		GDALDataType transform_datatype;
		double transform_offset;
		double transform_scale;
		std::string transform_offset_metadata;
		std::string transform_scale_metadata;

};



RasterDB::RasterDB(const char *_filename, bool writeable)
	: lockedfile(-1), writeable(writeable), filename_json(_filename), crs(nullptr), channelcount(0), channels(nullptr) {
	try {
		init();
	}
	catch (const std::exception &e) {
		cleanup();
		throw;
	}
}

RasterDB::~RasterDB() {
	cleanup();
}


void RasterDB::init() {
	size_t suffixpos = filename_json.rfind(".json");
	if (suffixpos == std::string::npos || suffixpos != filename_json.length()-5) {
		printf("Filename doesn't end with .json (%ld / %ld)\n", suffixpos, filename_json.length());
		throw SourceException("filename must end with .json");
	}
	std::string basename = filename_json.substr(0, suffixpos);
	filename_data = basename + ".dat";
	filename_db = basename + ".db";

	/*
	 * Step #1: open the .json file and parse it
	 */
	std::ifstream file(filename_json.c_str());
	if (!file.is_open()) {
		printf("unable to open file %s\n", filename_json.c_str());
		throw SourceException("unable to open file");
	}

	Json::Reader reader(Json::Features::strictMode());
	Json::Value root;
	if (!reader.parse(file, root)) {
		printf("unable to read json\n%s\n", reader.getFormattedErrorMessages().c_str());
		throw SourceException("unable to parse json");
	}

	file.close();

	// Now reopen the file to acquire a lock
	lockedfile = ::open(filename_json.c_str(), O_RDONLY | O_CLOEXEC); // | O_NOATIME | O_CLOEXEC);
	if (lockedfile < 0) {
		printf("Unable to open() rastersource at %s\n", filename_json.c_str());
		perror("error: ");
		throw SourceException("open() before flock() failed");
	}
	if (flock(lockedfile, writeable ? LOCK_EX : LOCK_SH) != 0) {
		printf("Unable to flock() rastersource\n");
		throw SourceException("flock() failed");
	}

	Json::Value jrm = root["coords"];
	Json::Value sizes = jrm["size"];
	Json::Value origins = jrm["origin"];
	Json::Value scales = jrm["scale"];
	int dimensions = sizes.size();
	if (dimensions != (int) origins.size() || dimensions != (int) scales.size())
		throw SourceException("json invalid, different dimensions in data");
	epsg_t epsg = (epsg_t) jrm.get("epsg", EPSG_UNKNOWN).asInt();

	if (dimensions == 2) {
		crs = new GDALCRS(
			epsg,
			sizes.get((Json::Value::ArrayIndex) 0, -1).asInt(), sizes.get((Json::Value::ArrayIndex) 1, -1).asInt(),
			origins.get((Json::Value::ArrayIndex) 0, 0).asInt(), origins.get((Json::Value::ArrayIndex) 1, 0).asInt(),
			scales.get((Json::Value::ArrayIndex) 0, 0).asDouble(), scales.get((Json::Value::ArrayIndex) 1, 0).asDouble()
		);
	}
	else
		throw SourceException("json invalid, can only process two-dimensional rasters");

	crs->verify();

	Json::Value channelinfo = root["channels"];
	if (!channelinfo.isArray() || channelinfo.size() < 1) {
		printf("No channel information in json\n");
		throw SourceException("No channel information in json");
	}

	channelcount = channelinfo.size();
	channels = new RasterDBChannel *[channelcount];
	for (int i=0;i<channelcount;i++)
		channels[i] = nullptr;

	for (int i=0;i<channelcount;i++) {
		Json::Value channel = channelinfo[(Json::Value::ArrayIndex) i];

		std::string datatype = channel.get("datatype", "unknown").asString();
		bool has_no_data = false;
		double no_data = 0;
		if (channel.isMember("nodata")) {
			has_no_data = true;
			no_data = channel.get("nodata", 0).asDouble();
		}

		channels[i] = new RasterDBChannel(DataDescription(
			GDALGetDataTypeByName(datatype.c_str()),
			channel.get("min", 0).asDouble(),
			channel.get("max", -1).asDouble(),
			has_no_data, no_data
		));
		if (channel.isMember("transform")) {
			Json::Value transform = channel["transform"];
			Json::Value offset = transform["offset"];
			Json::Value scale = transform["scale"];
			channels[i]->setTransform(
				GDALGetDataTypeByName(transform.get("datatype", "unknown").asString().c_str()),
				offset.type() != Json::stringValue ? offset.asDouble() : 0.0,
				scale.type()  != Json::stringValue ? scale.asDouble()  : 0.0,
				offset.type() == Json::stringValue ? offset.asString() : "",
				scale.type()  == Json::stringValue ? scale.asString()  : ""
			);

		}
		channels[i]->dd.verify();
	}

	/*
	 * Step #2: open the .db file and initialize it if needed
	 */
	db.open(filename_db.c_str());

	db.exec("CREATE TABLE IF NOT EXISTS rasters("
		" id INTEGER PRIMARY KEY,"
		" channel INTEGER NOT NULL,"
		" timestamp INTEGER NOT NULL,"
		" x1 INTEGER NOT NULL,"
		" y1 INTEGER NOT NULL,"
		" z1 INTERGET NOT NULL,"
		" x2 INTEGER NOT NULL,"
		" y2 INTEGER NOT NULL,"
		" z2 INTEGER NOT NULL,"
		" zoom INTEGER NOT NULL,"
		" filenr INTEGER NOT NULL,"
		" fileoffset INTEGER NOT NULL,"
		" filebytes INTEGER NOT NULL,"
		" compression INTEGER NOT NULL"
		")"
	);
	db.exec("CREATE UNIQUE INDEX IF NOT EXISTS ctxyzz ON rasters (channel, timestamp, x1, y1, z1, zoom)");

	db.exec("CREATE TABLE IF NOT EXISTS metadata("
		" id INTEGER PRIMARY KEY,"
		" channel INTEGER NOT NULL,"
		" timestamp INTEGER NOT NULL,"
		" isstring INTEGER NOT NULL,"
		" key STRING NOT NULL,"
		" value STRING NOT NULL"
		")"
	);

	db.exec("CREATE UNIQUE INDEX IF NOT EXISTS ctik ON metadata (channel, timestamp, isstring, key)");
}


void RasterDB::cleanup() {
	if (lockedfile != -1) {
		close(lockedfile); // also removes the lock acquired by flock()
		lockedfile = -1;
	}
	if (crs) {
		delete crs;
		crs = nullptr;
	}
	if (channelcount && channels) {
		for (int i=0;i<channelcount;i++) {
			delete channels[i];
		}
		delete [] channels;
	}
}



void RasterDB::import(const char *filename, int sourcechannel, int channelid, time_t timestamp, GenericRaster::Compression compression) {
	if (!isWriteable())
		throw SourceException("Cannot import into a source opened as read-only");
	bool raster_flipx, raster_flipy;
	auto raster = GenericRaster::fromGDAL(filename, sourcechannel, raster_flipx, raster_flipy, crs->epsg);

	bool crs_flipx, crs_flipy;
	SpatioTemporalReference stref(crs->epsg, crs->origin[0], crs->origin[1], crs->origin[0]+crs->scale[0], crs->origin[1]+crs->scale[1], crs_flipx, crs_flipy, TIMETYPE_UNREFERENCED, 0, 1);

	bool need_flipx = raster_flipx != crs_flipx;
	bool need_flipy = raster_flipy != crs_flipy;

	printf("GDAL: %d %d\nCRS:  %d %d\nflip: %d %d\n", raster_flipx, raster_flipy, crs_flipx, crs_flipy, need_flipx, need_flipy);

	if (need_flipx || need_flipy) {
		raster = raster->flip(need_flipx, need_flipy);
	}

	import(raster.get(), channelid, timestamp, compression);
}


void RasterDB::import(GenericRaster *raster, int channelid, time_t timestamp, GenericRaster::Compression compression) {
	if (!isWriteable())
		throw SourceException("Cannot import into a source opened as read-only");
	if (channelid < 0 || channelid >= channelcount)
		throw SourceException("RasterDB::import: unknown channel");
	/*
	if (!(raster->lcrs == *lcrs)) {
		std::cerr << "Imported CRS: " << raster->lcrs << std::endl;
		std::cerr << "Expected CRS: " << *lcrs << std::endl;
		throw SourceException("Local CRS does not match RasterDB");
	}
	*/

	// If the no_data value is missing in the import raster, we assume this to be a GDAL error.
	// In this case, we add the no_data value and continue as planned.
	DataDescription rasterdd = raster->dd;
	if (channels[channelid]->dd.has_no_data && !rasterdd.has_no_data) {
		rasterdd.has_no_data = true;
		rasterdd.no_data = channels[channelid]->dd.no_data;
	}
/*
	if (!(rasterdd == *channels[channelid])) {
		std::cerr << "imported raster: " << raster->dd << "expected:        " << *(channels[channelid]);
		throw SourceException("DataDescription does not match Channel's DataDescription");
	}
*/
	uint32_t tilesize = DEFAULT_TILE_SIZE;

	for (int zoom=0;;zoom++) {
		int zoomfactor = 1 << zoom;

		if (zoom > 0 && crs->size[0] / zoomfactor < tilesize && crs->size[1] / zoomfactor < tilesize && crs->size[2] / zoomfactor < tilesize)
			break;

		GenericRaster *zoomedraster = raster;
		std::unique_ptr<GenericRaster> zoomedraster_guard;
		if (zoom > 0) {
			printf("Scaling for zoom %d to %u x %u x %u pixels\n", zoom, crs->size[0] / zoomfactor, crs->size[1] / zoomfactor, crs->size[2] / zoomfactor);
			zoomedraster_guard = raster->scale(crs->size[0] / zoomfactor, crs->size[1] / zoomfactor, crs->size[2] / zoomfactor);
			zoomedraster = zoomedraster_guard.get();
			printf("done scaling\n");
		}

		/*for (uint32_t zoff = 0; zoff == 0 || zoff < zoomedraster->lcrs.size[2]; zoff += tilesize)*/ {
			uint32_t zoff = 0;
			uint32_t zsize = 0; //std::min(zoomedraster->lcrs.size[2] - zoff, tilesize);
			for (uint32_t yoff = 0; yoff == 0 || yoff < zoomedraster->height; yoff += tilesize) {
				uint32_t ysize = std::min(zoomedraster->height - yoff, tilesize);
				for (uint32_t xoff = 0; xoff < zoomedraster->width; xoff += tilesize) {
					uint32_t xsize = std::min(zoomedraster->width - xoff, tilesize);

					printf("importing tile at zoom %d with size %u: (%u, %u, %u) at offset (%u, %u, %u)\n", zoom, tilesize, xsize, ysize, zsize, xoff, yoff, zoff);
					if (hasTile(xsize, ysize, zsize, xoff*zoomfactor, yoff*zoomfactor, zoff*zoomfactor, zoom, channelid, timestamp)) {
						printf(" skipping..\n");
						continue;
					}

					//auto tile = GenericRaster::create(tilelcrs, channels[channelid]->dd);
					auto tile = GenericRaster::create(channels[channelid]->dd, SpatioTemporalReference::unreferenced(), xsize, ysize, zsize);
					tile->blit(zoomedraster, -xoff, -yoff, -zoff);
					printf("done blitting\n");

					importTile(tile.get(), xoff*zoomfactor, yoff*zoomfactor, zoff*zoomfactor, zoom, channelid, timestamp, compression);
					printf("done importing\n");
				}
			}
		}
	}

	SQLiteStatement stmt(db);
	stmt.prepare("INSERT INTO metadata (channel, timestamp, isstring, key, value) VALUES (?,?,?,?,?)");
	stmt.bind(1, channelid);
	stmt.bind(2, timestamp);
	stmt.bind(3, 1); // isstring

	// import metadata
	for (auto md : raster->md_string) {
		auto key = md.first;
		auto value = md.second;

		stmt.bind(4, key);
		stmt.bind(5, value);

		stmt.exec();
		printf("inserting string md: %s = %s\n", key.c_str(), value.c_str());
	}
	stmt.bind(3, 0); // isstring

	for (auto md : raster->md_value) {
		auto key = md.first;
		auto value = md.second;

		stmt.bind(4, key);
		stmt.bind(5, value);

		stmt.exec();
		printf("inserting value md: %s = %f\n", key.c_str(), value);
	}
}

bool RasterDB::hasTile(uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom, int channelid, time_t timestamp) {
	int zoomfactor = 1 << zoom;

	SQLiteStatement stmt(db);

	stmt.prepare("SELECT 1 FROM rasters WHERE channel = ? AND timestamp = ? AND x1 = ? AND y1 = ? AND z1 = ? AND x2 = ? AND y2 = ? AND z2 = ? AND zoom = ?");

	stmt.bind(1, channelid);
	stmt.bind(2, timestamp);
	stmt.bind(3, offx); // x1
	stmt.bind(4, offy); // y1
	stmt.bind(5, offz); // z1
	stmt.bind(6, (int32_t) (offx+width*zoomfactor)); // x2
	stmt.bind(7, (int32_t) (offy+height*zoomfactor)); // y2
	stmt.bind(8, (int32_t) (offz+depth*zoomfactor)); // z2
	stmt.bind(9, zoom);

	bool result = false;
	if (stmt.next())
		result = true;
	stmt.finalize();

	return result;
}

void RasterDB::importTile(GenericRaster *raster, int offx, int offy, int offz, int zoom, int channelid, time_t timestamp, GenericRaster::Compression compression) {
	auto buffer = RasterConverter::direct_encode(raster, compression);

	int zoomfactor = 1 << zoom;

	printf("Method %d, size of data: %ld -> %ld (%f)\n", (int) compression, raster->getDataSize(), buffer->size, (double) buffer->size / raster->getDataSize());

	// Step 1: compress and write
	size_t filenr = 0;

	FILE *f = fopen(filename_data.c_str(), "a+b");
	if (!f)
		throw SourceException("Could not open data file");

	if (fseek(f, 0, SEEK_END) != 0) {
		fclose(f);
		throw SourceException("tell failed");
	}
	long int fileoffset = ftell(f);
	if (fileoffset < 0) {
		fclose(f);
		throw SourceException("tell failed");
	}

	//printf("writing to file starting at position %ld\n", fileoffset);
	size_t written = fwrite(buffer->data, sizeof(unsigned char), buffer->size, f);

	fclose(f);
	if (written != buffer->size) {
		throw SourceException("writing failed, disk full?");
	}


	// Step 2: insert into DB
	SQLiteStatement stmt(db);

	stmt.prepare("INSERT INTO rasters (channel, timestamp, x1, y1, z1, x2, y2, z2, zoom, filenr, fileoffset, filebytes, compression)"
		" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");

	stmt.bind(1, channelid);
	stmt.bind(2, timestamp);
	stmt.bind(3, offx); // x1
	stmt.bind(4, offy); // y1
	stmt.bind(5, offz); // z1
	stmt.bind(6, (int32_t) (offx+raster->width*zoomfactor)); // x2
	stmt.bind(7, (int32_t) (offy+raster->height*zoomfactor)); // y2
	stmt.bind(8, (int32_t) 0/*(offz+raster->depth*zoomfactor)*/); // z2
	stmt.bind(9, zoom);
	stmt.bind(10, (int32_t) filenr);
	stmt.bind(11, (int64_t) fileoffset);
	stmt.bind(12, (int64_t) buffer->size);
	stmt.bind(13, compression);

	stmt.exec();
}


template<typename T1, typename T2>
struct raster_transformed_blit {
	static void execute(Raster2D<T1> *raster_dest, Raster2D<T2> *raster_src, int destx, int desty, int destz, double offset, double scale) {
		int x1 = std::max(destx, 0);
		int y1 = std::max(desty, 0);
		int x2 = std::min(raster_dest->width, destx+raster_src->width);
		int y2 = std::min(raster_dest->height, desty+raster_src->height);

		if (x1 >= x2 || y1 >= y2) {
			std::ostringstream msg;
			msg << "transformedBlit without overlapping region: " << raster_src->width << "x" << raster_src->height << " blitted onto " << raster_dest->width << "x" << raster_dest->height << " at (" << destx << "," << desty << "), overlap (" << x1 << "," << y1 << ") -> (" << x2 << "," << y2 << ")";
			throw ArgumentException(msg.str());
		}

		for (int y=y1;y<y2;y++) {
			for (int x=x1;x<x2;x++) {
				T2 val = raster_src->get(x-destx, y-desty);
				if (raster_src->dd.is_no_data(val))
					raster_dest->set(x, y, raster_dest->dd.no_data);
				else
					raster_dest->set(x, y, ((T1) val) * scale + offset);
			}
		}
	}
};


static void transformedBlit(GenericRaster *dest, GenericRaster *src, int destx, int desty, int destz, double offset, double scale) {
	if (src->getRepresentation() != GenericRaster::Representation::CPU || dest->getRepresentation() != GenericRaster::Representation::CPU)
		throw MetadataException("transformedBlit from raster that's not in a CPU buffer");

	callBinaryOperatorFunc<raster_transformed_blit>(dest, src, destx, desty, destz, offset, scale);
}

std::unique_ptr<GenericRaster> RasterDB::load(int channelid, time_t timestamp, int x1, int y1, int x2, int y2, int zoom, bool transform, size_t *io_cost) {
	if (io_cost)
		*io_cost = 0;

	if (channelid < 0 || channelid >= channelcount)
		throw SourceException("RasterDB::load: unknown channel");

	// find the most recent raster at the requested timestamp
	// TODO: maximal zulässige alter?
	SQLiteStatement stmt_t(db);
	stmt_t.prepare("SELECT timestamp FROM rasters WHERE channel = ? AND timestamp <= ? ORDER BY timestamp DESC limit 1");
	stmt_t.bind(1, channelid);
	stmt_t.bind(2, timestamp);
	if (!stmt_t.next())
		throw SourceException("No raster found for the given timestamp");

	timestamp = stmt_t.getInt64(0);
	stmt_t.finalize();

	// find the best available zoom level
	SQLiteStatement stmt_z(db);
	stmt_z.prepare("SELECT MAX(zoom) FROM rasters WHERE channel = ? AND timestamp = ? AND zoom <= ?");
	stmt_z.bind(1, channelid);
	stmt_z.bind(2, timestamp);
	stmt_z.bind(3, zoom);

	int max_zoom = -1;
	if (stmt_z.next())
		max_zoom = stmt_z.getInt(0);
	stmt_z.finalize();

	if (max_zoom < 0)
		throw SourceException("No zoom level found for the given channel and timestamp");

	zoom = std::min(zoom, max_zoom);
	int zoomfactor = 1 << zoom;


	// Figure out the CRS after cutting and zooming
	auto width = (x2-x1) >> zoom;
	auto height = (y2-y1) >> zoom;
	auto scale_x = crs->scale[0]*zoomfactor;
	auto scale_y = crs->scale[1]*zoomfactor;
	auto origin_x = crs->PixelToWorldX(x1);
	auto origin_y = crs->PixelToWorldY(y1);
	GDALCRS zoomed_and_cut_crs(crs->epsg, width, height, origin_x, origin_y, scale_x, scale_y);

	bool flipx, flipy;
	auto resultstref = zoomed_and_cut_crs.toSpatioTemporalReference(flipx, flipy, TIMETYPE_UNIX, timestamp, timestamp+1);

	/*
	if (x2 != x1 + (width << zoom) || y2 != y1 + (height << zoom)) {
		std::stringstream ss;
		ss << "RasterDB::load, fractions of a pixel requested: (x: " << x2 << " <-> " << (x1 + (width<<zoom)) << " y: " << y2 << " <-> " << (y1 + (height<<zoom));
		throw SourceException(ss.str());
	}
	*/
	// Make sure no fractional pixels are requested
	//x2 = x1 + (width << zoom);
	//y2 = y1 + (height << zoom);

	if (x1 > x2 || y1 > y2) {
		std::stringstream ss;
		ss << "RasterDB::load(" << channelid << ", " << timestamp << ", ["<<x1 <<"," << y1 <<" -> " << x2 << "," << y2 << "]): coords swapped";
		throw SourceException(ss.str());
	}

	// find all overlapping rasters in DB
	SQLiteStatement stmt(db);
	stmt.prepare("SELECT x1,y1,z1,x2,y2,z2,filenr,fileoffset,filebytes,compression FROM rasters"
		" WHERE channel = ? AND zoom = ? AND x1 < ? AND y1 < ? AND x2 > ? AND y2 > ? AND timestamp = ? ORDER BY filenr ASC, fileoffset ASC");

	stmt.bind(1, channelid);
	stmt.bind(2, zoom);
	stmt.bind(3, x2);
	stmt.bind(4, y2);
	stmt.bind(5, x1);
	stmt.bind(6, y1);
	stmt.bind(7, timestamp);

	decltype(GenericRaster::md_value) result_md_value;
	decltype(GenericRaster::md_string) result_md_string;
	SQLiteStatement stmt_md(db);
	stmt_md.prepare("SELECT isstring, key, value FROM metadata"
		" WHERE channel = ? AND timestamp = ?");
	stmt_md.bind(1, channelid);
	stmt_md.bind(2, timestamp);
	while (stmt_md.next()) {
		int isstring = stmt_md.getInt(0);
		std::string key(stmt_md.getString(1));
		const char *value = stmt_md.getString(2);
		if (isstring == 0) {
			double dvalue = std::strtod(value, nullptr);
			result_md_value.set(key, dvalue);
		}
		else
			result_md_string.set(key, std::string(value));
	}


	DataDescription transformed_dd = transform ? channels[channelid]->getTransformedDD(result_md_value) : channels[channelid]->dd;
	auto result = GenericRaster::create(transformed_dd, resultstref, width, height);
	result->clear(transformed_dd.no_data);

	// Load all overlapping parts and blit them onto the empty raster
	int tiles_found = 0;
	while (stmt.next()) {
		int r_x1 = stmt.getInt(0);
		int r_y1 = stmt.getInt(1);
		//int r_z1 = stmt.getInt(2);
		int r_x2 = stmt.getInt(3);
		int r_y2 = stmt.getInt(4);
		//int r_z2 = stmt.getInt(5);

		int fileid = stmt.getInt(6);
		size_t fileoffset = stmt.getInt64(7);
		size_t filebytes = stmt.getInt64(8);
		GenericRaster::Compression method = (GenericRaster::Compression) stmt.getInt(9);

		//fprintf(stderr, "loading raster from %d,%d, blitting to %d, %d\n", r_x1, r_y1, x1, y1);
		uint32_t tile_width = (r_x2-r_x1) >> zoom;
		uint32_t tile_height = (r_y2-r_y1) >> zoom;
		uint32_t tile_depth = 0;
		auto tile = loadTile(channelid, fileid, fileoffset, filebytes, tile_width, tile_height, tile_depth, method);
		if (io_cost)
			*io_cost += filebytes;

		if (transform && channels[channelid]->hasTransform()) {
			transformedBlit(
				result.get(), tile.get(),
				(r_x1-x1) >> zoom, (r_y1-y1) >> zoom, 0/* (r_z1-z1) >> zoom*/,
				channels[channelid]->getOffset(result_md_value), channels[channelid]->getScale(result_md_value));
		}
		else
			result->blit(tile.get(), (r_x1-x1) >> zoom, (r_y1-y1) >> zoom, 0/* (r_z1-z1) >> zoom*/);
		tiles_found++;
	}

	stmt.finalize();

	if (tiles_found == 0)
		throw SourceException("RasterDB::load(): No matching tiles found in DB");

	if (flipx || flipy) {
		result = result->flip(flipx, flipy);
	}

	result->md_value = std::move(result_md_value);
	result->md_string = std::move(result_md_string);
	result->md_value.set("Channel", channelid);
	return result;
}


std::unique_ptr<GenericRaster> RasterDB::loadTile(int channelid, int fileid, size_t offset, size_t size, uint32_t width, uint32_t height, uint32_t depth, GenericRaster::Compression method) {
	if (channelid < 0 || channelid >= channelcount)
		throw SourceException("RasterDB::load: unknown channel");

	//printf("loading raster from file %d offset %ld length %ld\n", fileid, offset, size);

#define USE_POSIX_IO true
#if USE_POSIX_IO
	int f = ::open(filename_data.c_str(), O_RDONLY | O_CLOEXEC); // | O_NOATIME
	if (f < 0)
		throw SourceException("Could not open data file");

	ByteBuffer buffer(size);
	if (lseek(f, (off_t) offset, SEEK_SET) != (off_t) offset) {
		close(f);
		throw SourceException("seek failed");
	}

	if (read(f, buffer.data, size) != (ssize_t) size) {
		close(f);
		throw SourceException("read failed");
	}
	close(f);
#else
	FILE *f = fopen(filename_data.c_str(), "rb");
	if (!f)
		throw SourceException("Could not open data file");

	if (fseek(f, offset, SEEK_SET) != 0) {
		fclose(f);
		throw SourceException("seek failed");
	}

	ByteBuffer buffer(size);
	if (fread(buffer.data, sizeof(unsigned char), size, f) != size) {
		fclose(f);
		throw SourceException("read failed");
	}
	fclose(f);
#endif

	// decode / decompress
	return RasterConverter::direct_decode(buffer, channels[channelid]->dd, SpatioTemporalReference::unreferenced(), width, height, depth, method);
}

std::unique_ptr<GenericRaster> RasterDB::query(const QueryRectangle &rect, QueryProfiler &profiler, int channelid, bool transform) {
	if (crs->epsg != rect.epsg) {
		std::stringstream msg;
		msg << "SourceOperator: wrong epsg requested. Source is " << (int) crs->epsg << ", requested " << (int) rect.epsg;
		throw OperatorException(msg.str());
	}

	// Get all pixel coordinates that need to be returned. The endpoints of the QueryRectangle are inclusive.
	double px1 = crs->WorldToPixelX(rect.x1);
	double py1 = crs->WorldToPixelY(rect.y1);
	double px2 = crs->WorldToPixelX(rect.x2);
	double py2 = crs->WorldToPixelY(rect.y2);

	// All Pixels even partially inside the rectangle need to be returned.
	int pixel_x1 = std::floor(std::min(px1,px2));
	int pixel_y1 = std::floor(std::min(py1,py2));
	int pixel_x2 = std::ceil(std::max(px1,px2))+1; // +1 because x2/y2 are not inclusive
	int pixel_y2 = std::ceil(std::max(py1,py2))+1;

	// Figure out the desired zoom level
	int zoom = 0;
	uint32_t pixel_width = pixel_x2 - pixel_x1;
	uint32_t pixel_height = pixel_y2 - pixel_y1;
	while (pixel_width > 2*rect.xres && pixel_height > 2*rect.yres) {
		zoom++;
		pixel_width >>= 1;
		pixel_height >>= 1;
	}

	size_t io_costs = 0;
	auto result = load(channelid, rect.timestamp, pixel_x1, pixel_y1, pixel_x2, pixel_y2, zoom, transform, &io_costs);
	profiler.addIOCost(io_costs);
	return result;
}




static std::unordered_map<std::string, std::weak_ptr<RasterDB> > RasterDB_map;
static std::mutex RasterDB_map_mutex;

std::shared_ptr<RasterDB> RasterDB::open(const char *filename, bool writeable) {
	std::lock_guard<std::mutex> guard(RasterDB_map_mutex);

	// To make sure each source is only accessed by a single path, resolve all symlinks
	char filename_clean[PATH_MAX];
	if (realpath(filename, filename_clean) == nullptr) {
		std::stringstream msg;
		msg << "realpath(\"" << filename << "\") failed";
		throw SourceException(msg.str());
	}

	std::string path(filename_clean);

	if (RasterDB_map.count(path) == 1) {
		auto &weak_ptr = RasterDB_map.at(path);
		auto shared_ptr = weak_ptr.lock();
		if (shared_ptr) {
			if (writeable && !shared_ptr->isWriteable())
				throw new SourceException("Cannot re-open source as read/write (TODO?)");
			return shared_ptr;
		}
		RasterDB_map.erase(path);
	}

	auto shared_ptr = std::make_shared<RasterDB>(path.c_str(), writeable);
	RasterDB_map[path] = std::weak_ptr<RasterDB>(shared_ptr);

	if (writeable && !shared_ptr->isWriteable())
		throw new SourceException("Cannot re-open source as read/write (TODO?)");
	return shared_ptr;
}

