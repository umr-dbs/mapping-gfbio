
#include "raster/raster.h"
#include "raster/rastersource.h"
#include "raster/profiler.h"
#include "raster/typejuggling.h"
#include "converters/converter.h"
#include "util/sqlite.h"


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



class RasterSourceChannel {
	public:
		//friend class RasterSource;

		RasterSourceChannel(const DataDescription &dd) : dd(dd), has_transform(false) {}
		~RasterSourceChannel() {}

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



RasterSource::RasterSource(const char *_filename, bool writeable)
	: lockedfile(-1), writeable(writeable), filename_json(_filename), lcrs(nullptr), channelcount(0), channels(nullptr), refcount(0) {
	try {
		init();
	}
	catch (const std::exception &e) {
		cleanup();
		throw;
	}
}

RasterSource::~RasterSource() {
	cleanup();
}


void RasterSource::init() {
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
	lockedfile = open(filename_json.c_str(), O_RDONLY | O_CLOEXEC); // | O_NOATIME | O_CLOEXEC);
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

	epsg_t epsg = jrm.get("epsg", EPSG_UNKNOWN).asInt();
	if (dimensions == 1) {
		lcrs = new LocalCRS(
			epsg,
			sizes.get((Json::Value::ArrayIndex) 0, -1).asInt(),
			origins.get((Json::Value::ArrayIndex) 0, 0).asInt(),
			scales.get((Json::Value::ArrayIndex) 0, 0).asDouble()
		);
	}
	else if (dimensions == 2) {
		lcrs = new LocalCRS(
			epsg,
			sizes.get((Json::Value::ArrayIndex) 0, -1).asInt(), sizes.get((Json::Value::ArrayIndex) 1, -1).asInt(),
			origins.get((Json::Value::ArrayIndex) 0, 0).asInt(), origins.get((Json::Value::ArrayIndex) 1, 0).asInt(),
			scales.get((Json::Value::ArrayIndex) 0, 0).asDouble(), scales.get((Json::Value::ArrayIndex) 1, 0).asDouble()
		);
	}
	else if (dimensions == 3) {
		lcrs = new LocalCRS(
			epsg,
			sizes.get((Json::Value::ArrayIndex) 0, -1).asInt(), sizes.get((Json::Value::ArrayIndex) 1, -1).asInt(), sizes.get((Json::Value::ArrayIndex) 2, -1).asInt(),
			origins.get((Json::Value::ArrayIndex) 0, 0).asInt(), origins.get((Json::Value::ArrayIndex) 1, 0).asInt(), origins.get((Json::Value::ArrayIndex) 2, 0).asInt(),
			scales.get((Json::Value::ArrayIndex) 0, 0).asDouble(), scales.get((Json::Value::ArrayIndex) 1, 0).asDouble(), scales.get((Json::Value::ArrayIndex) 2, 0).asDouble()
		);
	}
	else
		throw SourceException("json invalid, dimensions not between 1 and 3");

	lcrs->verify();

	Json::Value channelinfo = root["channels"];
	if (!channelinfo.isArray() || channelinfo.size() < 1) {
		printf("No channel information in json\n");
		throw SourceException("No channel information in json");
	}

	channelcount = channelinfo.size();
	channels = new RasterSourceChannel *[channelcount];
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

		channels[i] = new RasterSourceChannel(DataDescription(
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


void RasterSource::cleanup() {
	if (lockedfile != -1) {
		close(lockedfile); // also removes the lock acquired by flock()
		lockedfile = -1;
	}
	if (lcrs) {
		delete lcrs;
		lcrs = nullptr;
	}
	if (channelcount && channels) {
		for (int i=0;i<channelcount;i++) {
			delete channels[i];
		}
		delete [] channels;
	}
}



void RasterSource::import(const char *filename, int sourcechannel, int channelid, time_t timestamp, GenericRaster::Compression compression) {
	if (!isWriteable())
		throw SourceException("Cannot import into a source opened as read-only");
	auto raster = GenericRaster::fromGDAL(filename, sourcechannel, lcrs->epsg);

	import(raster.get(), channelid, timestamp, compression);
}


void RasterSource::import(GenericRaster *raster, int channelid, time_t timestamp, GenericRaster::Compression compression) {
	if (!isWriteable())
		throw SourceException("Cannot import into a source opened as read-only");
	if (channelid < 0 || channelid >= channelcount)
		throw SourceException("RasterSource::import: unknown channel");
	/*
	if (!(raster->lcrs == *lcrs)) {
		std::cerr << "Imported CRS: " << raster->lcrs << std::endl;
		std::cerr << "Expected CRS: " << *lcrs << std::endl;
		throw SourceException("Local CRS does not match RasterSource");
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

		if (zoom > 0 && lcrs->size[0] / zoomfactor < tilesize && lcrs->size[1] / zoomfactor < tilesize && lcrs->size[2] / zoomfactor < tilesize)
			break;

		GenericRaster *zoomedraster = raster;
		std::unique_ptr<GenericRaster> zoomedraster_guard;
		if (zoom > 0) {
			printf("Scaling for zoom %d to %u x %u x %u pixels\n", zoom, lcrs->size[0] / zoomfactor, lcrs->size[1] / zoomfactor, lcrs->size[2] / zoomfactor);
			zoomedraster_guard = raster->scale(lcrs->size[0] / zoomfactor, lcrs->size[1] / zoomfactor, lcrs->size[2] / zoomfactor);
			zoomedraster = zoomedraster_guard.get();
			printf("done scaling\n");
		}

		for (uint32_t zoff = 0; zoff == 0 || zoff < zoomedraster->lcrs.size[2]; zoff += tilesize) {
			uint32_t zsize = std::min(zoomedraster->lcrs.size[2] - zoff, tilesize);
			for (uint32_t yoff = 0; yoff == 0 || yoff < zoomedraster->lcrs.size[1]; yoff += tilesize) {
				uint32_t ysize = std::min(zoomedraster->lcrs.size[1] - yoff, tilesize);
				for (uint32_t xoff = 0; xoff < zoomedraster->lcrs.size[0]; xoff += tilesize) {
					uint32_t xsize = std::min(zoomedraster->lcrs.size[0] - xoff, tilesize);

					LocalCRS tilelcrs(lcrs->epsg, lcrs->dimensions,
						xsize, ysize, zsize,
						zoomedraster->lcrs.PixelToWorldX(xoff), zoomedraster->lcrs.PixelToWorldY(yoff), zoomedraster->lcrs.PixelToWorldZ(zoff),
						zoomedraster->lcrs.scale[0], zoomedraster->lcrs.scale[1], zoomedraster->lcrs.scale[2]
					);

					printf("importing tile at zoom %d with size %u: (%u, %u, %u) at offset (%u, %u, %u)\n", zoom, tilesize, xsize, ysize, zsize, xoff, yoff, zoff);
					if (hasTile(tilelcrs, xoff*zoomfactor, yoff*zoomfactor, zoff*zoomfactor, zoom, channelid, timestamp)) {
						printf(" skipping..\n");
						continue;
					}

					auto tile = GenericRaster::create(tilelcrs, channels[channelid]->dd);
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
	//importTile(raster, 0, 0, 0, 0, channelid, timestamp, compression);
}

bool RasterSource::hasTile(const LocalCRS &lcrs, int offx, int offy, int offz, int zoom, int channelid, time_t timestamp) {
	int zoomfactor = 1 << zoom;

	SQLiteStatement stmt(db);

	stmt.prepare("SELECT 1 FROM rasters WHERE channel = ? AND timestamp = ? AND x1 = ? AND y1 = ? AND z1 = ? AND x2 = ? AND y2 = ? AND z2 = ? AND zoom = ?");

	stmt.bind(1, channelid);
	stmt.bind(2, timestamp);
	stmt.bind(3, offx); // x1
	stmt.bind(4, offy); // y1
	stmt.bind(5, offz); // z1
	stmt.bind(6, (int32_t) (offx+lcrs.size[0]*zoomfactor)); // x2
	stmt.bind(7, (int32_t) (offy+lcrs.size[1]*zoomfactor)); // y2
	stmt.bind(8, (int32_t) (offz+lcrs.size[2]*zoomfactor)); // z2
	stmt.bind(9, zoom);

	bool result = false;
	if (stmt.next())
		result = true;
	stmt.finalize();

	return result;
}

void RasterSource::importTile(GenericRaster *raster, int offx, int offy, int offz, int zoom, int channelid, time_t timestamp, GenericRaster::Compression compression) {
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
	stmt.bind(6, (int32_t) (offx+raster->lcrs.size[0]*zoomfactor)); // x2
	stmt.bind(7, (int32_t) (offy+raster->lcrs.size[1]*zoomfactor)); // y2
	stmt.bind(8, (int32_t) (offz+raster->lcrs.size[2]*zoomfactor)); // z2
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
		int x2 = std::min(raster_dest->lcrs.size[0], destx+raster_src->lcrs.size[0]);
		int y2 = std::min(raster_dest->lcrs.size[1], desty+raster_src->lcrs.size[1]);

		if (x1 >= x2 || y1 >= y2)
			throw MetadataException("transformedBlit without overlapping region");

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
	if (src->lcrs.dimensions != 2 || dest->lcrs.dimensions != 2 || src->lcrs.epsg != dest->lcrs.epsg)
		throw MetadataException("transformedBlit with incompatible rasters");

	if (src->getRepresentation() != GenericRaster::Representation::CPU || dest->getRepresentation() != GenericRaster::Representation::CPU)
		throw MetadataException("transformedBlit from raster that's not in a CPU buffer");

	callBinaryOperatorFunc<raster_transformed_blit>(dest, src, destx, desty, destz, offset, scale);
}

std::unique_ptr<GenericRaster> RasterSource::load(int channelid, time_t timestamp, int x1, int y1, int x2, int y2, int zoom, bool transform) {
	if (channelid < 0 || channelid >= channelcount)
		throw SourceException("RasterSource::load: unknown channel");

	Profiler::Profiler p_all("RasterSource::load");

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

/*
	x1 = std::max(x1, 0);
	y1 = std::max(y1, 0);
	x2 = std::min(x2, (int) lcrs->size[0]);
	y2 = std::min(y2, (int) lcrs->size[1]);
*/
	/*
	if (x1 < 0 || y1 < 0 || (size_t) x2 > lcrs->size[0] || (size_t) y2 > lcrs->size[1]) {
		std::stringstream ss;
		ss << "RasterSource::load(" << channelid << ", " << timestamp << ", ["<<x1 <<"," << y1 <<" -> " << x2 << "," << y2 << "]): coords out of bounds";
		throw SourceException(ss.str());
	}
	*/

	if (x1 > x2 || y1 > y2) {
		std::stringstream ss;
		ss << "RasterSource::load(" << channelid << ", " << timestamp << ", ["<<x1 <<"," << y1 <<" -> " << x2 << "," << y2 << "]): coords swapped";
		throw SourceException(ss.str());
	}

	// find all overlapping rasters in DB
	Profiler::start("RasterSource::load: sqlite");
	SQLiteStatement stmt(db);
	stmt.prepare("SELECT x1,y1,z1,x2,y2,z2,filenr,fileoffset,filebytes,compression FROM rasters"
		" WHERE channel = ? AND zoom = ? AND x1 < ? AND y1 < ? AND x2 >= ? AND y2 >= ? AND timestamp = ?");

	stmt.bind(1, channelid);
	stmt.bind(2, zoom);
	stmt.bind(3, x2);
	stmt.bind(4, y2);
	stmt.bind(5, x1);
	stmt.bind(6, y1);
	stmt.bind(7, timestamp);
	Profiler::stop("RasterSource::load: sqlite");

	Profiler::start("RasterSource::load: metadata");
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
	Profiler::stop("RasterSource::load: metadata");
	Profiler::start("RasterSource::load: create");

	// Create an empty raster of the desired size
	LocalCRS resultmetadata(
		lcrs->epsg,
		lcrs->dimensions,
		(x2-x1) >> zoom, (y2-y1) >> zoom, 0 /* (z2-z1) >> zoom */,
		lcrs->PixelToWorldX(x1), lcrs->PixelToWorldY(y1), lcrs->PixelToWorldZ(0 /* z1 */),
		lcrs->scale[0]*zoomfactor, lcrs->scale[1]*zoomfactor, lcrs->scale[2]*zoomfactor
	);

	DataDescription transformed_dd = transform ? channels[channelid]->getTransformedDD(result_md_value) : channels[channelid]->dd;
	auto result = GenericRaster::create(resultmetadata, transformed_dd);
	result->clear(transformed_dd.no_data);
	result->md_value = std::move(result_md_value);
	result->md_string = std::move(result_md_string);
	Profiler::stop("RasterSource::stop: create");

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
		LocalCRS tilelcrs(
			lcrs->epsg,
			lcrs->dimensions,
			(r_x2-r_x1) >> zoom, (r_y2-r_y1) >> zoom, 0 /* (r_z2-r_z1) >> zoom */,
			lcrs->PixelToWorldX(r_x1), lcrs->PixelToWorldY(r_y1), lcrs->PixelToWorldZ(0 /* r_z1 */),
			lcrs->scale[0]*zoomfactor, lcrs->scale[1]*zoomfactor, lcrs->scale[2]*zoomfactor
		);
		auto tile = loadTile(channelid, tilelcrs, fileid, fileoffset, filebytes, method);
		Profiler::start("RasterSource::load: blit");

		if (transform && channels[channelid]->hasTransform()) {
			transformedBlit(
				result.get(), tile.get(),
				(r_x1-x1) >> zoom, (r_y1-y1) >> zoom, 0/* (r_z1-z1) >> zoom*/,
				channels[channelid]->getOffset(result->md_value), channels[channelid]->getScale(result->md_value));
		}
		else
			result->blit(tile.get(), (r_x1-x1) >> zoom, (r_y1-y1) >> zoom, 0/* (r_z1-z1) >> zoom*/);
		Profiler::stop("RasterSource::load: blit");
		tiles_found++;
	}

	stmt.finalize();

	if (tiles_found == 0)
		throw SourceException("RasterSource::load(): No matching tiles found in DB");

	result->md_value.set("Channel", channelid);
	return result;
}


std::unique_ptr<GenericRaster> RasterSource::loadTile(int channelid, const LocalCRS &tilecrs, int fileid, size_t offset, size_t size, GenericRaster::Compression method) {
	if (channelid < 0 || channelid >= channelcount)
		throw SourceException("RasterSource::load: unknown channel");

	//printf("loading raster from file %d offset %ld length %ld\n", fileid, offset, size);

	Profiler::start("RasterSource::load: File IO");
#define USE_POSIX_IO true
#if USE_POSIX_IO
	int f = open(filename_data.c_str(), O_RDONLY | O_CLOEXEC); // | O_NOATIME
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
	Profiler::stop("RasterSource::load: File IO");

	// decode / decompress
	Profiler::Profiler p("RasterSource::load: decompress");
	return RasterConverter::direct_decode(tilecrs, channels[channelid]->dd, &buffer, method);
}





std::unordered_map<std::string, RasterSource *> RasterSourceManager::map;
std::mutex RasterSourceManager::mutex;

RasterSource *RasterSourceManager::open(const char *filename, bool writeable)
{
	std::lock_guard<std::mutex> guard(mutex);

	// To make sure each source is only accessed by a single path, resolve all symlinks
	char filename_clean[PATH_MAX];
	if (realpath(filename, filename_clean) == nullptr)
		throw new SourceException("realpath() failed");

	std::string path(filename_clean);

	RasterSource *source = nullptr;
	if (map.count(path) == 1) {
		source = map.at(path);
	}
	else {
		source = new RasterSource(path.c_str(), writeable);
		map[path] = source;
	}

	if (writeable && !source->isWriteable())
		throw new SourceException("Cannot re-open source as read/write (TODO?)");

	source->refcount++;
	return source;
}

void RasterSourceManager::close(RasterSource *source)
{
	if (!source)
		return;
	std::lock_guard<std::mutex> guard(mutex);

	source->refcount--;
	if (source->refcount <= 0)
		delete source;
}


