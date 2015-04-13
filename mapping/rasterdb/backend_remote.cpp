
#include "rasterdb/backend_remote.h"

#include "util/binarystream.h"
#include "util/configuration.h"
#include "util/make_unique.h"

#include <stdio.h>
#include <cstdlib>
#include <sstream>
#include <fstream>

RemoteRasterDBBackend::RemoteRasterDBBackend(const char *sourcename, bool writeable)
	: RasterDBBackend(writeable), sourcename(sourcename) {
	if (writeable)
		throw ArgumentException("RemoteRasterDBBackend cannot be opened writeable");

	auto servername = Configuration::get("rasterdb.remote.host");
	auto serverport = Configuration::get("rasterdb.remote.port");
	cache_directory = Configuration::get("rasterdb.remote.cache", "");

	int portnr = atoi(serverport.c_str());

	//printf("Connecting to %s port %d\n", servername.c_str(), portnr);

	stream.reset( new UnixSocket(servername.c_str(), portnr) );

	auto c = COMMAND_OPEN;
	stream->write(c);
	stream->write(this->sourcename);
	uint8_t response;
	stream->read(&response);
	if (response != 48)
		throw NetworkException("RemoteRasterDBBackend: COMMAND_OPEN failed");
}

RemoteRasterDBBackend::~RemoteRasterDBBackend() {
	// TODO: send "end"-command?
}


std::string RemoteRasterDBBackend::readJSON() {
	if (json.size() == 0) {
		auto cmd = this->COMMAND_READJSON;
		stream->write( cmd );
		stream->read(&json);
	}
	return json;
}


RasterDBBackend::RasterDescription RemoteRasterDBBackend::getClosestRaster(int channelid, double timestamp) {
	auto c = COMMAND_GETCLOSESTRASTER;
	stream->write(c);
	stream->write(channelid);
	stream->write(timestamp);
	RasterDescription res(*stream);
	if (res.rasterid < 0) {
		std::string error;
		stream->read(&error);
		throw SourceException(error);
	}
	return res;
}

void RemoteRasterDBBackend::readAttributes(rasterid rasterid, DirectMetadata<std::string> &md_string, DirectMetadata<double> &md_value) {
	auto c = COMMAND_READATTRIBUTES;
	stream->write(c);
	stream->write(rasterid);
	// read strings
	while (true) {
		std::string key;
		stream->read(&key);
		if (key == "")
			break;
		std::string value;
		stream->read(&value);
		md_string.set(key, value);
	}
	// read values
	while (true) {
		std::string key;
		stream->read(&key);
		if (key == "")
			break;
		double value;
		stream->read(&value);
		md_value.set(key, value);
	}
}

int RemoteRasterDBBackend::getBestZoom(rasterid rasterid, int desiredzoom) {
	auto c = COMMAND_GETBESTZOOM;
	stream->write(c);
	stream->write(rasterid);
	stream->write(desiredzoom);
	int bestzoom;
	stream->read(&bestzoom);
	return bestzoom;
}

const std::vector<RasterDBBackend::TileDescription> RemoteRasterDBBackend::enumerateTiles(int channelid, rasterid rasterid, int x1, int y1, int x2, int y2, int zoom) {
	auto c = COMMAND_ENUMERATETILES;
	stream->write(c);
	stream->write(channelid);
	stream->write(rasterid);
	stream->write(x1);
	stream->write(y1);
	stream->write(x2);
	stream->write(y2);
	stream->write(zoom);
	std::vector<TileDescription> result;
	size_t count;
	stream->read(&count);
	for (size_t i=0;i<count;i++)
		result.push_back(TileDescription(*stream));
	return result;
}

bool RemoteRasterDBBackend::hasTile(rasterid rasterid, uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom) {
	throw std::runtime_error("RemoteRasterDBBackend::hasTile() not implemented");
}

std::unique_ptr<ByteBuffer> RemoteRasterDBBackend::readTile(const TileDescription &tiledesc) {
	std::string cachepath;
	if (cache_directory != "") {
		std::ostringstream pathss;
		pathss << cache_directory << sourcename << "_" << tiledesc.channelid << "_" << tiledesc.tileid << ".tile";
		cachepath = pathss.str();

		std::ifstream file(cachepath);
		if (file.is_open()) {
			file.seekg(0, std::ios::end);
			auto filesize = file.tellg();
			if (filesize == tiledesc.size) {
				file.seekg(0, std::ios::beg);
				auto bb = std::make_unique<ByteBuffer>(filesize);
				file.read((char *) bb->data, bb->size);
				return bb;
			}
			else {
				fprintf(stderr, "RemoteRasterDBBackend::readTile(): size in cache %lu, expected %lu\n", filesize, tiledesc.size);
				file.close();
			}
		}
	}

	auto c = COMMAND_READTILE;
	stream->write(c);
	stream->write(tiledesc);
	size_t size;
	stream->read(&size);

	auto bb = std::make_unique<ByteBuffer>(size);
	stream->read((char *) bb->data, bb->size);

	if (cache_directory != "") {
		std::ofstream file(cachepath);
		file.write((char *) bb->data, bb->size);
		file.close();
	}
	return bb;
}
