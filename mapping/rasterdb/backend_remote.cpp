
#include "rasterdb/backend_remote.h"

#include "util/binarystream.h"
#include "util/configuration.h"
#include "util/make_unique.h"

#include <stdio.h>
#include <cstdlib>
#include <sstream>
#include <fstream>

RemoteRasterDBBackend::RemoteRasterDBBackend() {
	auto servername = Configuration::get("rasterdb.remote.host");
	auto serverport = Configuration::get("rasterdb.remote.port");
	cache_directory = Configuration::get("rasterdb.remote.cache", "");

	int portnr = atoi(serverport.c_str());

	//printf("Connecting to %s port %d\n", servername.c_str(), portnr);

	stream.reset( new BinaryFDStream(servername.c_str(), portnr, true) );
}

RemoteRasterDBBackend::~RemoteRasterDBBackend() {
	// TODO: send "end"-command?
}

std::vector<std::string> RemoteRasterDBBackend::enumerateSources() {
	auto c = COMMAND_ENUMERATESOURCES;
	BinaryWriteBuffer buffer;
	buffer.write(c);
	buffer.prepareForWriting();
	stream->write(buffer);
	stream->flush();

	std::vector<std::string> sourcenames;
	size_t count;
	stream->read(&count);
	for (size_t i=0;i<count;i++) {
		std::string name;
		stream->read(&name);
		sourcenames.push_back(name);
	}
	return sourcenames;
}
std::string RemoteRasterDBBackend::readJSON(const std::string &sourcename) {
	auto c = COMMAND_READANYJSON;
	BinaryWriteBuffer buffer;
	buffer.write(c);
	buffer.write(sourcename);
	buffer.prepareForWriting();
	stream->write(buffer);
	stream->flush();

	std::string json;
	stream->read(&json);
	return json;
}


void RemoteRasterDBBackend::open(const std::string &_sourcename, bool writeable) {
	if (this->is_opened)
		throw ArgumentException("Cannot open RemoteRasterDBBackend twice");
	if (writeable)
		throw ArgumentException("RemoteRasterDBBackend cannot be opened writeable");

	sourcename = _sourcename;
	is_writeable = writeable;

	auto c = COMMAND_OPEN;
	BinaryWriteBuffer buffer;
	buffer.write(c);
	buffer.write(this->sourcename);
	buffer.prepareForWriting();
	stream->write(buffer);
	stream->flush();

	uint8_t response;
	stream->read(&response);
	if (response != 48)
		throw NetworkException("RemoteRasterDBBackend: COMMAND_OPEN failed");

	is_opened = true;
}


std::string RemoteRasterDBBackend::readJSON() {
	if (!this->is_opened)
		throw ArgumentException("Cannot call readJSON() before open() on a RasterDBBackend");

	if (json.size() == 0) {
		auto c = COMMAND_READJSON;
		BinaryWriteBuffer buffer;
		buffer.write(c);
		buffer.prepareForWriting();
		stream->write(buffer);
		stream->flush();

		stream->read(&json);
	}
	return json;
}


RasterDBBackend::RasterDescription RemoteRasterDBBackend::getClosestRaster(int channelid, double t1, double t2) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call getClosestRaster() before open() on a RasterDBBackend");

	auto c = COMMAND_GETCLOSESTRASTER;
	BinaryWriteBuffer buffer;
	buffer.write(c);
	buffer.write(channelid);
	buffer.write(t1);
	buffer.write(t2);
	buffer.prepareForWriting();
	stream->write(buffer);
	stream->flush();


	RasterDescription res(*stream);
	if (res.rasterid < 0) {
		std::string error;
		stream->read(&error);
		throw SourceException(error);
	}
	return res;
}

void RemoteRasterDBBackend::readAttributes(rasterid_t rasterid, AttributeMaps &attributes) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call readAttributes() before open() on a RasterDBBackend");

	auto c = COMMAND_READATTRIBUTES;
	BinaryWriteBuffer buffer;
	buffer.write(c);
	buffer.write(rasterid);
	buffer.prepareForWriting();
	stream->write(buffer);
	stream->flush();

	// read strings
	while (true) {
		std::string key;
		stream->read(&key);
		if (key == "")
			break;
		std::string value;
		stream->read(&value);
		attributes.setTextual(key, value);
	}
	// read values
	while (true) {
		std::string key;
		stream->read(&key);
		if (key == "")
			break;
		double value;
		stream->read(&value);
		attributes.setNumeric(key, value);
	}
}

int RemoteRasterDBBackend::getBestZoom(rasterid_t rasterid, int desiredzoom) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call getBestZoom() before open() on a RasterDBBackend");

	auto c = COMMAND_GETBESTZOOM;
	BinaryWriteBuffer buffer;
	buffer.write(c);
	buffer.write(rasterid);
	buffer.write(desiredzoom);
	buffer.prepareForWriting();
	stream->write(buffer);
	stream->flush();

	int bestzoom;
	stream->read(&bestzoom);
	return bestzoom;
}

const std::vector<RasterDBBackend::TileDescription> RemoteRasterDBBackend::enumerateTiles(int channelid, rasterid_t rasterid, int x1, int y1, int x2, int y2, int zoom) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call enumerateTiles() before open() on a RasterDBBackend");

	auto c = COMMAND_ENUMERATETILES;
	BinaryWriteBuffer buffer;
	buffer.write(c);
	buffer.write(channelid);
	buffer.write(rasterid);
	buffer.write(x1);
	buffer.write(y1);
	buffer.write(x2);
	buffer.write(y2);
	buffer.write(zoom);
	buffer.prepareForWriting();
	stream->write(buffer);
	stream->flush();

	std::vector<TileDescription> result;
	size_t count;
	stream->read(&count);
	for (size_t i=0;i<count;i++)
		result.push_back(TileDescription(*stream));
	return result;
}

bool RemoteRasterDBBackend::hasTile(rasterid_t rasterid, uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom) {
	throw std::runtime_error("RemoteRasterDBBackend::hasTile() not implemented");
}

std::unique_ptr<ByteBuffer> RemoteRasterDBBackend::readTile(const TileDescription &tiledesc) {
	if (!this->is_opened)
		throw ArgumentException("Cannot call readTile() before open() on a RasterDBBackend");

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
				auto bb = make_unique<ByteBuffer>(filesize);
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
	BinaryWriteBuffer buffer;
	buffer.write(c);
	buffer.write(tiledesc);
	buffer.prepareForWriting();
	stream->write(buffer);
	stream->flush();

	size_t size;
	stream->read(&size);

	auto bb = make_unique<ByteBuffer>(size);
	stream->read((char *) bb->data, bb->size);

	if (cache_directory != "") {
		std::ofstream file(cachepath);
		file.write((char *) bb->data, bb->size);
		file.close();
	}
	return bb;
}
