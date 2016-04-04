#include "util/exceptions.h"
#include "util/binarystream.h"
#include "rserver/rserver.h"

#include "util/server_nonblocking.h"

#include "datatypes/raster.h"
#include "datatypes/raster/raster_priv.h"
#include "datatypes/plots/text.h"
#include "datatypes/pointcollection.h"
#include "raster/profiler.h"
#include "operators/operator.h"
#include "util/make_unique.h"
#include "util/log.h"
#include "util/configuration.h"

#include <cstdlib>
#include <cstdio>
#include <string.h> // memset()
#include <map>
#include <atomic>
#include <iostream>
#include <fstream>

#include <time.h>

#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h> // waitpid()


#ifdef __clang__ // Prevent GCC from complaining about unknown pragmas.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter" // silence the myriad of warnings in ***REMOVED*** headers
#endif // __clang__
#include <***REMOVED***Common.h>

#include "rserver/***REMOVED***_wrapper.h" // declarations only

#include <***REMOVED***.h>
#include <***REMOVED***.h>
#if !defined(RINSIDE_CALLBACKS)
#error "***REMOVED*** was not compiled with RINSIDE_CALLBACKS"
#endif
#if !defined(RCPP_USING_CXX11)
#error "***REMOVED*** didn't detect C++11 support (RCPP_USING_CXX11 is not defined)"
#endif

#pragma clang diagnostic pop // ignored "-Wunused-parameter"


***REMOVED***::Function *attributes = nullptr;
#include "rserver/***REMOVED***_wrapper.h" // definitions


#include "rserver/***REMOVED***_callbacks.h"


// Set to true while you're sending. If an exception happens when not sending, an error message can be returned
std::atomic<bool> is_sending(false);


std::unique_ptr<GenericRaster> query_raster_source(BinaryStream &stream, int childidx, const QueryRectangle &rect) {
	Profiler::Profiler("requesting Raster");
	Log::debug("requesting raster %d with rect (%f,%f -> %f,%f)", childidx, rect.x1,rect.y1, rect.x2,rect.y2);
	is_sending = true;
	BinaryWriteBuffer response;
	response.write((char) RSERVER_TYPE_RASTER);
	response.write(childidx);
	response.write(rect);
	stream.write(response);
	is_sending = false;

	BinaryReadBuffer new_request;
	stream.read(new_request);
	auto raster = GenericRaster::deserialize(new_request);
	raster->setRepresentation(GenericRaster::Representation::CPU);
	return raster;
}

***REMOVED***::NumericVector query_raster_source_as_array(BinaryStream &stream, int childidx, const QueryRectangle &rect) {
	auto raster = query_raster_source(stream, childidx, rect);

	// convert to vector
	raster->setRepresentation(GenericRaster::Representation::CPU);
	int width = raster->width;
	int height = raster->height;
	***REMOVED***::NumericVector pixels(raster->getPixelCount());
	int pos = 0;
	for (int y=0;y<height;y++) {
		for (int x=0;x<width;x++) {
			double val = raster->getAsDouble(x, y);
			if (raster->dd.is_no_data(val))
				pixels[pos++] = NAN;
			else
				pixels[pos++] = val;
		}
	}
	return pixels;
}

std::unique_ptr<PointCollection> query_points_source(BinaryStream &stream, int childidx, const QueryRectangle &rect) {
	Profiler::Profiler("requesting Points");
	Log::debug("requesting points %d with rect (%f,%f -> %f,%f)", childidx, rect.x1,rect.y1, rect.x2,rect.y2);
	is_sending = true;
	BinaryWriteBuffer response;
	response.write((char) RSERVER_TYPE_POINTS);
	response.write(childidx);
	response.write(rect);
	stream.write(response);
	is_sending = false;

	BinaryReadBuffer new_request;
	stream.read(new_request);
	auto points = make_unique<PointCollection>(new_request);
	return points;
}


static std::string read_file_as_string(const std::string &filename) {
	std::ifstream in(filename, std::ios::in | std::ios::binary);
	if (in) {
		std::string contents;
		in.seekg(0, std::ios::end);
		contents.resize(in.tellg());
		in.seekg(0, std::ios::beg);
		in.read(&contents[0], contents.size());
		in.close();
		return contents;
	}
	throw PlatformException("Could not read plot file");
}


void signal_handler(int signum) {
	Log::error("Caught signal %d, exiting", signum);
	exit(signum);
}


class RServerConnection : public NonblockingServer::Connection {
	public:
		RServerConnection(NonblockingServer &server, int fd, int id);
		~RServerConnection();
	private:
		virtual void processData(std::unique_ptr<BinaryReadBuffer> request);
		virtual void processDataForked(BinaryStream stream);
		std::string source;
		char expected_result;
		int rastersourcecount;
		int pointssourcecount;
		QueryRectangle qrect;
};

class RServer : public NonblockingServer {
	public:
		RServer(***REMOVED*** *R, ***REMOVED***Callbacks *callbacks) : NonblockingServer(),
			R(R), callbacks(callbacks) {
		}
		virtual ~RServer() {};
	private:
		virtual std::unique_ptr<Connection> createConnection(int fd, int id);

		***REMOVED*** *R;
		***REMOVED***Callbacks *callbacks;
		friend class RServerConnection;
};


RServerConnection::RServerConnection(NonblockingServer &server, int fd, int id) : Connection(server, fd, id),
	source(""), expected_result(-1), rastersourcecount(-1), pointssourcecount(-1), qrect(SpatialReference::unreferenced(), TemporalReference::unreferenced(), QueryResolution::none()) {
	Log::info("%d: connected", id);
}

RServerConnection::~RServerConnection() {
}

void RServerConnection::processData(std::unique_ptr<BinaryReadBuffer> request) {
	int magic = request->read<int>();
	if (magic != RSERVER_MAGIC_NUMBER)
		throw PlatformException("Client sent the wrong magic number");
	expected_result = request->read<char>();
	Log::info("Requested type: %d", expected_result);
	request->read(&source);
	rastersourcecount = request->read<int>();
	pointssourcecount = request->read<int>();
	Log::info("Requested counts: %d %d", rastersourcecount, pointssourcecount);
	qrect = QueryRectangle(*request);
	Log::info("rectangle is rect (%f,%f -> %f,%f)", qrect.x1,qrect.y1, qrect.x2,qrect.y2);

	auto timeout = request->read<int>();
	forkAndProcess(timeout);
}

void RServerConnection::processDataForked(BinaryStream stream) {
	RServer &rserver = (RServer &) server;
	***REMOVED*** &R = *(rserver.R);

	Log::info("Here's our client!");

	if (expected_result == RSERVER_TYPE_PLOT) {
		R.parseEval("rserver_plot_tempfile = tempfile(\"rs_plot\", fileext=\".png\")");
		R.parseEval("png(rserver_plot_tempfile, width=1000, height=1000, bg=\"transparent\")");
	}

	R["mapping.rastercount"] = rastersourcecount;
	std::function<std::unique_ptr<GenericRaster>(int, const QueryRectangle &)> bound_raster_source = std::bind(query_raster_source, std::ref(stream), std::placeholders::_1, std::placeholders::_2);
	R["mapping.loadRaster"] = ***REMOVED***::InternalFunction( bound_raster_source );
	std::function<***REMOVED***::NumericVector(int, const QueryRectangle &)> bound_raster_source_as_array = std::bind(query_raster_source_as_array, std::ref(stream), std::placeholders::_1, std::placeholders::_2);
	R["mapping.loadRasterAsVector"] = ***REMOVED***::InternalFunction( bound_raster_source_as_array );

	std::function<std::unique_ptr<PointCollection>(int, const QueryRectangle &)> bound_points_source = std::bind(query_points_source, std::ref(stream), std::placeholders::_1, std::placeholders::_2);
	R["mapping.pointscount"] = pointssourcecount;
	R["mapping.loadPoints"] = ***REMOVED***::InternalFunction( bound_points_source );

	R["mapping.qrect"] = qrect;

	Profiler::start("running R script");
	try {
		std::string delimiter = "\n\n";
		size_t start = 0;
		size_t end = 0;
		while (true) {
			end = source.find(delimiter, start);
			if (end == std::string::npos)
				break;
			std::string line = source.substr(start, end-start);
			start = end+delimiter.length();
			Log::debug("src: %s", line.c_str());
			R.parseEval(line);
		}
		std::string lastline = source.substr(start);
		Log::info("src: %s", lastline.c_str());
		auto result = R.parseEval(lastline);
		Profiler::stop("running R script");

		if (expected_result == RSERVER_TYPE_RASTER) {
			auto raster = ***REMOVED***::as<std::unique_ptr<GenericRaster>>(result);
			is_sending = true;
			BinaryWriteBuffer response;
			response.write((char) -RSERVER_TYPE_RASTER);
			response.write(*raster, true);
			stream.write(response);
		}
		else if (expected_result == RSERVER_TYPE_POINTS) {
			auto points = ***REMOVED***::as<std::unique_ptr<PointCollection>>(result);
			is_sending = true;
			BinaryWriteBuffer response;
			response.write((char) -RSERVER_TYPE_POINTS);
			response.write(*points, true);
			stream.write(response);
		}
		else if (expected_result == RSERVER_TYPE_STRING) {
			std::string output = rserver.callbacks->getConsoleOutput();
			is_sending = true;
			BinaryWriteBuffer response;
			response.write((char) -RSERVER_TYPE_STRING);
			response.write(output, true);
			stream.write(response);
		}
		else if (expected_result == RSERVER_TYPE_PLOT) {
			R.parseEval("dev.off()");
			std::string filename = ***REMOVED***::as<std::string>(R["rserver_plot_tempfile"]);
			std::string output = read_file_as_string(filename);
			std::remove(filename.c_str());
			is_sending = true;
			BinaryWriteBuffer response;
			response.write((char) -RSERVER_TYPE_PLOT);
			response.write(output, true);
			stream.write(response);
		}
		else
			throw PlatformException("Unknown result type requested");
	}
	catch (const NetworkException &e) {
		// don't do anything
		throw;
	}
	catch (const std::exception &e) {
		// We're already in the middle of sending something to the client. We cannot send more data in the middle of a packet.
		if (is_sending)
			throw;

		auto what = e.what();
		Log::warn("Exception: %s", what);
		std::string msg(what);
		BinaryWriteBuffer response;
		response.write((char) -RSERVER_TYPE_ERROR);
		response.write(msg);
		stream.write(response);
		return;
	}
}


std::unique_ptr<NonblockingServer::Connection> RServer::createConnection(int fd, int id) {
	return make_unique<RServerConnection>(*this, fd, id);
}


int main()
{
	Configuration::loadFromDefaultPaths();

	auto portnr = Configuration::getInt("rserver.port");

	Log::setLogFd(stdout);
	Log::setLevel(Configuration::get("rserver.loglevel", "info"));

	// Signal handlers
	int signals[] = {SIGHUP, SIGINT, 0};
	for (int i=0;signals[i];i++) {
		if (signal(signals[i], signal_handler) == SIG_ERR) {
			Log::error("Cannot install signal handler: %s", strerror(errno));
			exit(1);
		}
		else
			Log::debug("Signal handler for %d installed", signals[i]);
	}
	signal(SIGPIPE, SIG_IGN);

	// Initialize R environment
	***REMOVED***Callbacks *Rcallbacks = new ***REMOVED***Callbacks();
	Log::info("...loading R");
	***REMOVED*** R;
	R.set_callbacks( Rcallbacks );

	Log::info("...loading packages");

	std::string packages = Configuration::get("rserver.packages", "");
	try {
		std::string::size_type pos = 0;
		while (pos != std::string::npos) {
			auto next_pos = packages.find(",", pos);
			auto name = packages.substr(pos, next_pos == std::string::npos ? next_pos : next_pos-pos);
			Log::debug("Loading package '%s'", name.c_str());
			std::string command = "library(\"" + name + "\")";
			R.parseEvalQ(command);
			pos = next_pos;
			if (pos != std::string::npos)
				pos++;
		}
	}
	catch (const std::exception &e) {
		Log::error("error loading packages: %s", e.what());
		Log::error("R's output:\n", Rcallbacks->getConsoleOutput().c_str());
		exit(5);
	}
	Rcallbacks->resetConsoleOutput();

	Log::info("Capturing functions..");
	***REMOVED***::Function _attributes("attributes");
	attributes = &_attributes;

	Log::info("R is ready, starting server..");

	RServer server(&R, Rcallbacks);
	//server.listen(rserver_socket, 0777);
	server.listen(portnr);
	server.setWorkerThreads(0);
	server.allowForking();
	server.start();

	return 0;
}
