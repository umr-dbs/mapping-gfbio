#include "util/exceptions.h"
#include "rasterdb/backend.h"
#include "rasterdb/backend_local.h"
#include "rasterdb/backend_remote.h"
#include "util/configuration.h"
#include "util/make_unique.h"
#include "util/server_nonblocking.h"
#include "util/log.h"

class TileServerConnection : public NonblockingServer::Connection {
	public:
		TileServerConnection(int fd, int id);
		~TileServerConnection();
	private:
		virtual std::unique_ptr<BinaryWriteBuffer> processRequest(NonblockingServer &server, std::unique_ptr<BinaryReadBuffer> request);
		std::shared_ptr<LocalRasterDBBackend> backend;
};

TileServerConnection::TileServerConnection(int fd, int id) : Connection(fd, id) {
	Log::info("%d: connected", id);
	backend = make_unique<LocalRasterDBBackend>();
}

TileServerConnection::~TileServerConnection() {

}

std::unique_ptr<BinaryWriteBuffer> TileServerConnection::processRequest(NonblockingServer &server, std::unique_ptr<BinaryReadBuffer> request) {
	uint8_t OK = 48;

	uint8_t c;
	if (!request->read(&c, true)) {
		Log::info("%d: disconnected", id);
		return nullptr;
	}

	Log::info("%d: got command %d", id, c);

	if (backend == nullptr && c >= RemoteRasterDBBackend::FIRST_SOURCE_SPECIFIC_COMMAND)
		return nullptr;

	auto response = make_unique<BinaryWriteBuffer>();

	switch (c) {
		case RemoteRasterDBBackend::COMMAND_EXIT: {
			return nullptr;
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
			RasterDBBackend::TileDescription tile(*request);
			Log::info("%d: returning tile, offset %lu, size %lu", id, tile.offset, tile.size);

			// To avoid copying the bytebuffer with the tiledata, we're linking the data in the writebuffer.
			// To do this, our bytebuffer's lifetime must be managed by the writebuffer.
			// Unfortunately, we have to replace our freshly allocated writebuffer with a different one right here.
			BinaryWriteBufferWithObject<ByteBuffer> *response2 = new BinaryWriteBufferWithObject<ByteBuffer>();
			response.reset(response2);
			response2->object = backend->readTile(tile);
			response2->write(response2->object->size);
			response2->enableLinking();
			response2->write((const char *) response2->object->data, response2->object->size, true);
			response2->disableLinking();
			Log::info("%d: data sent\n", id);
			break;
		}
		default: {
			Log::info("%d: got unknown command %d, disconnecting\n", id, c);
			return nullptr;
		}
	}
	response->prepareForWriting();
	Log::info("%d: response of %d bytes", id, (int) response->getSize());
	if (response->getSize() == 0)
		throw ArgumentException("No response written");
	return response;
}


class TileServer : public NonblockingServer {
	public:
		using NonblockingServer::NonblockingServer;
		virtual ~TileServer() {};
	private:
		virtual std::unique_ptr<Connection> createConnection(int fd, int id);
};

std::unique_ptr<NonblockingServer::Connection> TileServer::createConnection(int fd, int id) {
	return make_unique<TileServerConnection>(fd, id);
}


int main(void) {
	Configuration::loadFromDefaultPaths();
	Log::setLogFd(stdout);
	Log::setLevel("trace");

	auto portstr = Configuration::get("rasterdb.tileserver.port");
	auto portnr = atoi(portstr.c_str());

	Log::info("server: listening on port %d", portnr);

	TileServer server;
	server.listen(portnr);
	server.start();

	return 0;
}
