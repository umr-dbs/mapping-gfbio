
#include "rasterdb/backend_local.h"
#include "util/configuration.h"
#include "util/make_unique.h"


#include <sys/file.h> // flock()
#include <sys/types.h> // the next three are for posix open()
#include <sys/stat.h>
#include <fcntl.h>

#include <iostream>
#include <fstream>

#include <dirent.h>


LocalRasterDBBackend::LocalRasterDBBackend() : lockedfile(-1) {
}

LocalRasterDBBackend::~LocalRasterDBBackend() {
	cleanup();
}

static bool has_suffix(const std::string &str, const std::string &suffix) {
	if (suffix.length() > str.length())
		return false;
	return str.compare(str.length() - suffix.length(), suffix.length(), suffix) == 0;
}

std::vector<std::string> LocalRasterDBBackend::enumerateSources() {
	std::vector<std::string> sourcenames;

	auto path = Configuration::get("rasterdb.local.path", "");

	std::string suffix = ".json";

	DIR *dir = opendir(path.c_str());
	struct dirent *ent;
	if (dir == nullptr)
		throw ArgumentException(concat("Could not open path for enumerating: ", path));

	while ((ent = readdir(dir)) != nullptr) {
		std::string name = ent->d_name;
		if (has_suffix(name, suffix))
			sourcenames.push_back(name.substr(0, name.length() - suffix.length()));
	}
	closedir(dir);

	return sourcenames;
}
std::string LocalRasterDBBackend::readJSON(const std::string &sourcename) {
	std::string basepath = Configuration::get("rasterdb.local.path", "") + sourcename;
	filename_json = basepath + ".json";
	std::ifstream file(filename_json.c_str());
	if (!file.is_open())
		throw SourceException("unable to open .json file");

	std::string json;
	file.seekg(0, std::ios::end);
	json.resize(file.tellg());
	file.seekg(0, std::ios::beg);
	file.read(&json[0], json.size());
	file.close();
	return json;
}

void LocalRasterDBBackend::open(const std::string &_sourcename, bool writeable) {
	if (this->is_opened)
		throw ArgumentException("Cannot open LocalRasterDBBackend twice");

	sourcename = _sourcename;
	is_writeable = writeable;

	std::string basepath = Configuration::get("rasterdb.local.path", "") + sourcename;

	filename_json = basepath + ".json";
	filename_data = basepath + ".dat";
	filename_db = basepath + ".db";

	/*
	 * Step #1: open the .json file and store its contents
	 */
	std::ifstream file(filename_json.c_str());
	if (!file.is_open()) {
		fprintf(stderr, "unable to open file %s\n", filename_json.c_str());
		throw SourceException("unable to open file");
	}

	file.seekg(0, std::ios::end);
	json.resize(file.tellg());
	file.seekg(0, std::ios::beg);
	file.read(&json[0], json.size());
	file.close();

	// Now reopen the file to acquire a lock
	lockedfile = ::open(filename_json.c_str(), O_RDONLY | O_CLOEXEC); // | O_NOATIME | O_CLOEXEC);
	if (lockedfile < 0) {
		fprintf(stderr, "Unable to open() rastersource at %s\n", filename_json.c_str());
		perror("error: ");
		throw SourceException("open() before flock() failed");
	}
	if (flock(lockedfile, is_writeable ? LOCK_EX : LOCK_SH) != 0) {
		fprintf(stderr, "Unable to flock() rastersource\n");
		throw SourceException("flock() failed");
	}


	/*
	 * Step #2: open the .db file and initialize it if needed
	 */
	db.open(filename_db.c_str(), !writeable);

	if (writeable) {
		db.exec("CREATE TABLE IF NOT EXISTS rasters("
			" id INTEGER PRIMARY KEY,"
			" channel INTEGER NOT NULL,"
			" time_start REAL NOT NULL,"
			" time_end REAL NOT NULL"
			")"
		);
		db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_ct ON rasters (channel, time_start)");

		db.exec("CREATE TABLE IF NOT EXISTS tiles("
			" id INTEGER PRIMARY KEY,"
			" rasterid INTEGER NOT NULL,"
			" x1 INTEGER NOT NULL,"
			" y1 INTEGER NOT NULL,"
			" z1 INTEGER NOT NULL,"
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
		db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_rxyzz ON tiles (rasterid, x1, y1, z1, zoom)");

		db.exec("CREATE TABLE IF NOT EXISTS attributes("
			" rasterid INTEGER NOT NULL,"
			" isstring INTEGER NOT NULL,"
			" key STRING NOT NULL,"
			" value STRING NOT NULL"
			")"
		);
		db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_rik ON attributes (rasterid, isstring, key)");
	}

	is_opened = true;
}

void LocalRasterDBBackend::cleanup() {
	if (lockedfile != -1) {
		close(lockedfile); // also removes the lock acquired by flock()
		lockedfile = -1;
	}
}

std::string LocalRasterDBBackend::readJSON() {
	if (!this->is_opened)
		throw ArgumentException("Cannot call readJSON() before open() on a RasterDBBackend");
	return json;
}

RasterDBBackend::rasterid_t LocalRasterDBBackend::createRaster(int channel, double time_start, double time_end, const AttributeMaps &attributes) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call createRaster() before open() on a RasterDBBackend");

	auto stmt = db.prepare("SELECT id FROM rasters WHERE channel = ? AND ABS(time_start - ?) < 0.001 AND ABS(time_end - ?) < 0.001");
	stmt.bind(1, channel);
	stmt.bind(2, time_start);
	stmt.bind(3, time_end);
	if (stmt.next()) {
		std::cerr << "createRaster: returning existing raster for " << time_start << " -> " << time_end << "\n";
		return stmt.getInt64(0);
	}
	stmt.finalize();

	stmt = db.prepare("INSERT INTO rasters (channel, time_start, time_end) VALUES (?,?,?)");
	stmt.bind(1, channel);
	stmt.bind(2, time_start);
	stmt.bind(3, time_end);
	stmt.exec();
	auto rasterid = db.getLastInsertId();
	stmt.finalize();

	stmt = db.prepare("INSERT INTO attributes (rasterid, isstring, key, value) VALUES (?,?,?,?)");
	stmt.bind(1, rasterid);
	stmt.bind(2, 1); // isstring

	// import metadata
	for (auto attr : attributes.textual()) {
		auto key = attr.first;
		auto value = attr.second;

		stmt.bind(3, key);
		stmt.bind(4, value);

		stmt.exec();
		printf("inserting textual attribute: %s = %s\n", key.c_str(), value.c_str());
	}
	stmt.bind(2, 0); // isstring

	for (auto attr : attributes.numeric()) {
		auto key = attr.first;
		auto value = attr.second;

		stmt.bind(3, key);
		stmt.bind(4, value);

		stmt.exec();
		printf("inserting numeric attribute: %s = %f\n", key.c_str(), value);
	}

	return rasterid;
}

void LocalRasterDBBackend::writeTile(rasterid_t rasterid, ByteBuffer &buffer, uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom, RasterConverter::Compression compression) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call writeTile() before open() on a RasterDBBackend");

	int zoomfactor = 1 << zoom;

	// Step 1: write data to disk
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
	size_t written = fwrite(buffer.data, sizeof(unsigned char), buffer.size, f);

	fclose(f);
	if (written != buffer.size)
		throw SourceException("writing failed, disk full?");

	// Step 2: insert into DB
	auto stmt = db.prepare("INSERT INTO tiles (rasterid, x1, y1, z1, x2, y2, z2, zoom, filenr, fileoffset, filebytes, compression)"
		" VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");

	stmt.bind(1, rasterid);
	stmt.bind(2, offx); // x1
	stmt.bind(3, offy); // y1
	stmt.bind(4, offz); // z1
	stmt.bind(5, (int32_t) (offx+width*zoomfactor)); // x2
	stmt.bind(6, (int32_t) (offy+height*zoomfactor)); // y2
	stmt.bind(7, (int32_t) 0/*(offz+depth*zoomfactor)*/); // z2
	stmt.bind(8, zoom);
	stmt.bind(9, (int32_t) filenr);
	stmt.bind(10, (int64_t) fileoffset);
	stmt.bind(11, (int64_t) buffer.size);
	stmt.bind(12, compression);

	stmt.exec();
}

void LocalRasterDBBackend::linkRaster(int channelid, double time_of_reference, double time_start, double time_end) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call linkRaster() before open() on a RasterDBBackend");

	auto rd = getClosestRaster(channelid, time_of_reference, time_of_reference);

	if (time_end > rd.time_start && time_start < rd.time_end)
		throw SourceException("Cannot link rasters with overlapping time intervals");

	// Create the new raster
	auto stmt = db.prepare("INSERT INTO rasters (channel, time_start, time_end) VALUES (?,?,?)");
	stmt.bind(1, channelid);
	stmt.bind(2, time_start);
	stmt.bind(3, time_end);
	stmt.exec();
	auto rasterid = db.getLastInsertId();
	stmt.finalize();

	// Copy all attributes
	auto stmt_attr = db.prepare("INSERT INTO attributes (rasterid, isstring, key, value) SELECT ? AS rasterid, isstring, key, value FROM attributes WHERE rasterid = ?");
	stmt_attr.bind(1, rasterid);
	stmt_attr.bind(2, rd.rasterid);
	stmt_attr.exec();

	// Copy all tiles
	// Note: this will assign new IDs to the copies, so they will be stored twice in the tileserver cache
	auto stmt_tiles = db.prepare("INSERT INTO tiles (rasterid, x1, y1, z1, x2, y2, z2, zoom, filenr, fileoffset, filebytes, compression)"
		" SELECT ? AS rasterid, x1, y1, z1, x2, y2, z2, zoom, filenr, fileoffset, filebytes, compression FROM tiles WHERE rasterid = ?");
	stmt_tiles.bind(1, rasterid);
	stmt_tiles.bind(2, rd.rasterid);
	stmt_tiles.exec();
}


RasterDBBackend::RasterDescription LocalRasterDBBackend::getClosestRaster(int channelid, double t1, double t2) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call getClosestRaster() before open() on a RasterDBBackend");

	// find a raster that's valid during the given timestamp
	auto stmt = db.prepare("SELECT id, time_start, time_end FROM rasters WHERE channel = ? AND time_start <= ? AND time_end >= ? ORDER BY time_start DESC limit 1");
	stmt.bind(1, channelid);
	stmt.bind(2, t1);
	stmt.bind(3, t2);
	if (!stmt.next())
		throw SourceException( concat("No raster found for the given time (source=", sourcename, ", channel=", channelid, ", time=", t1, "-", t2, ")"));

	auto rasterid = stmt.getInt64(0);
	double time_start = stmt.getDouble(1);
	double time_end = stmt.getDouble(2);
	stmt.finalize();
	return RasterDescription{rasterid, time_start, time_end};
}

void LocalRasterDBBackend::readAttributes(rasterid_t rasterid, AttributeMaps &attributes) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call readAttributes() before open() on a RasterDBBackend");

	auto stmt_md = db.prepare("SELECT isstring, key, value FROM attributes WHERE rasterid = ?");
	stmt_md.bind(1, rasterid);
	while (stmt_md.next()) {
		int isstring = stmt_md.getInt(0);
		std::string key(stmt_md.getString(1));
		const char *value = stmt_md.getString(2);
		if (isstring == 0) {
			double dvalue = std::strtod(value, nullptr);
			attributes.setNumeric(key, dvalue);
		}
		else
			attributes.setTextual(key, std::string(value));
	}
}

int LocalRasterDBBackend::getBestZoom(rasterid_t rasterid, int desiredzoom) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call getBestZoom() before open() on a RasterDBBackend");

	auto stmt_z = db.prepare("SELECT MAX(zoom) FROM tiles WHERE rasterid = ? AND zoom <= ?");
	stmt_z.bind(1, rasterid);
	stmt_z.bind(2, desiredzoom);

	int max_zoom = -1;
	if (stmt_z.next())
		max_zoom = stmt_z.getInt(0);
	stmt_z.finalize();

	if (max_zoom < 0)
		throw SourceException("No zoom level found for the given channel and timestamp");

	return std::min(desiredzoom, max_zoom);
}

const std::vector<RasterDBBackend::TileDescription> LocalRasterDBBackend::enumerateTiles(int channelid, rasterid_t rasterid, int x1, int y1, int x2, int y2, int zoom) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call enumerateTiles() before open() on a RasterDBBackend");

	std::vector<TileDescription> result;

	// find all overlapping rasters in DB
	auto stmt = db.prepare("SELECT id,x1,y1,z1,x2,y2,z2,filenr,fileoffset,filebytes,compression FROM tiles"
		" WHERE rasterid = ? AND zoom = ? AND x1 < ? AND y1 < ? AND x2 > ? AND y2 > ? ORDER BY filenr ASC, fileoffset ASC");

	stmt.bind(1, rasterid);
	stmt.bind(2, zoom);
	stmt.bind(3, x2);
	stmt.bind(4, y2);
	stmt.bind(5, x1);
	stmt.bind(6, y1);

	while (stmt.next()) {
		tileid_t tileid = stmt.getInt64(0);
		uint32_t r_x1 = stmt.getInt(1);
		uint32_t r_y1 = stmt.getInt(2);
		//uint32_t r_z1 = stmt.getInt(3);
		uint32_t r_x2 = stmt.getInt(4);
		uint32_t r_y2 = stmt.getInt(5);
		//uint32_t r_z2 = stmt.getInt(6);

		int fileid = stmt.getInt(7);
		size_t fileoffset = stmt.getInt64(8);
		size_t filebytes = stmt.getInt64(9);
		RasterConverter::Compression method = (RasterConverter::Compression) stmt.getInt(10);

		uint32_t tile_width = (r_x2-r_x1) >> zoom;
		uint32_t tile_height = (r_y2-r_y1) >> zoom;
		uint32_t tile_depth = 0;

		result.push_back(TileDescription{tileid, channelid, fileid, fileoffset, filebytes, r_x1, r_y1, 0, tile_width, tile_height, tile_depth, method});
	}

	stmt.finalize();
	return result;
}

bool LocalRasterDBBackend::hasTile(rasterid_t rasterid, uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call hasTile() before open() on a RasterDBBackend");

	int zoomfactor = 1 << zoom;

	auto stmt = db.prepare("SELECT 1 FROM tiles WHERE rasterid = ? AND x1 = ? AND y1 = ? AND z1 = ? AND x2 = ? AND y2 = ? AND z2 = ? AND zoom = ?");

	stmt.bind(1, rasterid);
	stmt.bind(2, offx); // x1
	stmt.bind(3, offy); // y1
	stmt.bind(4, offz); // z1
	stmt.bind(5, (int32_t) (offx+width*zoomfactor)); // x2
	stmt.bind(6, (int32_t) (offy+height*zoomfactor)); // y2
	stmt.bind(7, (int32_t) (offz+depth*zoomfactor)); // z2
	stmt.bind(8, zoom);

	bool result = false;
	if (stmt.next())
		result = true;
	stmt.finalize();

	return result;
}

std::unique_ptr<ByteBuffer> LocalRasterDBBackend::readTile(const TileDescription &tiledesc) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call readTile() before open() on a RasterDBBackend");

#define USE_POSIX_IO true
#if USE_POSIX_IO
	int f = ::open(filename_data.c_str(), O_RDONLY | O_CLOEXEC); // | O_NOATIME
	if (f < 0)
		throw SourceException("Could not open data file");

	auto buffer = make_unique<ByteBuffer>(tiledesc.size);
	if (lseek(f, (off_t) tiledesc.offset, SEEK_SET) != (off_t) tiledesc.offset) {
		close(f);
		throw SourceException("seek failed");
	}

	if (read(f, buffer->data, tiledesc.size) != (ssize_t) tiledesc.size) {
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

	auto buffer = make_unique<ByteBuffer>(tiledesc.size);
	if (fread(buffer->data, sizeof(unsigned char), tiledesc.size, f) != tiledesc.size) {
		fclose(f);
		throw SourceException("read failed");
	}
	fclose(f);
#endif
	return buffer;
}
