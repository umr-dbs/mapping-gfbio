#include "util/exceptions.h"
#include "rasterdb/backend.h"
#include "rasterdb/backend_remote.h"
#include "util/configuration.h"
#include "util/make_unique.h"
#include "util/server_nonblocking.h"
#include "util/log.h"

#include <iostream>

class TileServerConnection : public NonblockingServer::Connection {
	public:
		TileServerConnection(NonblockingServer &server, int fd, int id);
		~TileServerConnection();
	private:
		virtual void processData(std::unique_ptr<BinaryReadBuffer> request);
		virtual void processDataAsync();
		std::shared_ptr<RasterDBBackend> backend;
		std::unique_ptr<RasterDBBackend::TileDescription> tile;
};

TileServerConnection::TileServerConnection(NonblockingServer &server, int fd, int id) : Connection(server, fd, id) {
	Log::info("%d: connected", id);
	backend = RasterDBBackend::create("local", Configuration::get("rasterdb.local.location"), Parameters());
}

TileServerConnection::~TileServerConnection() {

}

void TileServerConnection::processData(std::unique_ptr<BinaryReadBuffer> request) {
	uint8_t OK = 48;

	auto c = request->read<uint8_t>();

	Log::info("%d: got command %d", id, c);

	if (backend == nullptr && c >= RemoteRasterDBBackend::FIRST_SOURCE_SPECIFIC_COMMAND) {
		close();
		return;
	}

	auto response = make_unique<BinaryWriteBuffer>();

	switch (c) {
		case RemoteRasterDBBackend::COMMAND_EXIT: {
			close();
			return;
			break;
		}
		case RemoteRasterDBBackend::COMMAND_ENUMERATESOURCES: {
			std::vector<std::string> sourcenames = backend->enumerateSources();

			size_t size = sourcenames.size();
			response->write(size);
			for (const auto &name : sourcenames)
				response->write(name);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_READANYJSON: {
			std::string name;
			request->read(&name);

			auto json = backend->readJSON(name);
			response->write(json);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_OPEN: {
			if (backend->isOpen())
				throw NetworkException("Cannot call open() twice!");
			std::string path;
			request->read(&path);

			backend->open(path, false);
			response->write(OK);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_READJSON: {
			auto json = backend->readJSON();
			response->write(json);
			break;
		}
		//case RemoteRasterDBBackend::COMMAND_CREATERASTER:
		//case RemoteRasterDBBackend::COMMAND_WRITETILE:
		case RemoteRasterDBBackend::COMMAND_GETCLOSESTRASTER: {
			int channelid;
			request->read(&channelid);
			double t1, t2;
			request->read(&t1);
			request->read(&t2);

			try {
				auto res = backend->getClosestRaster(channelid, t1, t2);
				Log::info("%d: found closest raster with id %ld, time %f->%f", id, res.rasterid, res.time_start, res.time_end);
				response->write(res);
			}
			catch (const SourceException &e) {
				RasterDBBackend::RasterDescription r(-1, 0, 0);
				response->write(r);
				std::string error(e.what());
				response->write(error);
			}
			break;
		}
		case RemoteRasterDBBackend::COMMAND_READATTRIBUTES: {
			RasterDBBackend::rasterid_t rasterid;
			request->read(&rasterid);
			AttributeMaps attributes;
			backend->readAttributes(rasterid, attributes);

			std::string empty("");
			for (auto pair : attributes.textual()) {
				response->write(pair.first);
				response->write(pair.second);
			}
			response->write(empty);
			for (auto pair : attributes.numeric()) {
				response->write(pair.first);
				response->write(pair.second);
			}
			response->write(empty);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_GETBESTZOOM: {
			RasterDBBackend::rasterid_t rasterid;
			request->read(&rasterid);
			int desiredzoom;
			request->read(&desiredzoom);

			int bestzoom = backend->getBestZoom(rasterid, desiredzoom);
			response->write(bestzoom);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_ENUMERATETILES: {
			int channelid;
			request->read(&channelid);
			RemoteRasterDBBackend::rasterid_t rasterid;
			request->read(&rasterid);
			int x1, y1, x2, y2, zoom;
			request->read(&x1);
			request->read(&y1);
			request->read(&x2);
			request->read(&y2);
			request->read(&zoom);

			auto res = backend->enumerateTiles(channelid, rasterid, x1, y1, x2, y2, zoom);
			size_t size = res.size();
			Log::info("%d: (%d,%d) -> (%d,%d), channel %d, raster %ld at zoom %d yielded %lu tiles", id, x1, y1, x2, y2, channelid, rasterid, zoom, size);
			response->write(size);
			for (auto &td : res)
				response->write(td);
			break;
		}
		//case RemoteRasterDBBackend::COMMAND_HASTILE:
		case RemoteRasterDBBackend::COMMAND_READTILE: {
			tile = make_unique<RasterDBBackend::TileDescription>(*request);
			Log::info("%d: returning tile, offset %lu, size %lu", id, tile->offset, tile->size);
			enqueueForAsyncProcessing();
			return;
		}
		default: {
			Log::info("%d: got unknown command %d, disconnecting", id, c);
			close();
			return;
		}
	}
	startWritingData(std::move(response));
}

void TileServerConnection::processDataAsync() {
	// only one request is handled asynchronously: READTILE
	// `tile` is set to the TileDescription read from the request

	// To avoid copying the bytebuffer with the tiledata, we're linking the data in the writebuffer.
	// To do this, our bytebuffer's lifetime must be managed by the writebuffer.
	// Unfortunately, we have to replace our freshly allocated writebuffer with a different one right here.
	auto response = make_unique<BinaryWriteBufferWithObject<ByteBuffer>>();
	response->object = backend->readTile(*tile);
	tile.reset(nullptr);
	response->write(response->object->size);
	response->write((const char *) response->object->data, response->object->size, true);
	Log::info("%d: data sent", id);
	startWritingData(std::move(response));
}


class TileServer : public NonblockingServer {
	public:
		using NonblockingServer::NonblockingServer;
		virtual ~TileServer() {};
	private:
		virtual std::unique_ptr<Connection> createConnection(int fd, int id);
};

std::unique_ptr<NonblockingServer::Connection> TileServer::createConnection(int fd, int id) {
	return make_unique<TileServerConnection>(*this, fd, id);
}


int main(void) {
	Configuration::loadFromDefaultPaths();

	Log::logToStream(Configuration::get("rasterdb.tileserver.loglevel", "info"), &std::cout);

	auto portnr = Configuration::getInt("rasterdb.tileserver.port");

	auto threadsstr = Configuration::get("rasterdb.tileserver.threads", "1");
	auto threads = std::max(1, atoi(threadsstr.c_str()));

	Log::info("server: listening on port %d, using %d worker threads", portnr, threads);

	TileServer server;
	server.listen(portnr);
	server.setWorkerThreads(threads);
	server.start();

	return 0;
}
