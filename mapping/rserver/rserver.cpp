#include "util/exceptions.h"
#include "util/binarystream.h"
#include "rserver/rserver.h"

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

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <poll.h>
#include <signal.h>


const int TIMEOUT_SECONDS = 600;

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


int cmpTimespec(const struct timespec &t1, const struct timespec &t2) {
	if (t1.tv_sec < t2.tv_sec)
		return -1;
	if (t1.tv_sec > t2.tv_sec)
		return 1;
	if (t1.tv_nsec < t2.tv_nsec)
		return -1;
	if (t1.tv_nsec > t2.tv_nsec)
		return 1;
	return 0;
}


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


void client(int sock_fd, ***REMOVED*** &R, ***REMOVED***Callbacks &Rcallbacks) {
	auto stream = BinaryStream::fromAcceptedSocket(sock_fd);

	BinaryReadBuffer request;
	stream.read(request);
	int magic = request.read<int>();;
	if (magic != RSERVER_MAGIC_NUMBER)
		throw PlatformException("Client sent the wrong magic number");
	auto type = request.read<char>();
	Log::info("Requested type: %d", type);
	std::string source;
	request.read(&source);
	auto rastersourcecount = request.read<int>();
	auto pointssourcecount = request.read<int>();
	Log::info("Requested counts: %d %d", rastersourcecount, pointssourcecount);
	QueryRectangle qrect(request);
	Log::info("rectangle is rect (%f,%f -> %f,%f)", qrect.x1,qrect.y1, qrect.x2,qrect.y2);

	if (type == RSERVER_TYPE_PLOT) {
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

		if (type == RSERVER_TYPE_RASTER) {
			auto raster = ***REMOVED***::as<std::unique_ptr<GenericRaster>>(result);
			is_sending = true;
			BinaryWriteBuffer response;
			response.write((char) -RSERVER_TYPE_RASTER);
			response.write(*raster, true);
			stream.write(response);
		}
		else if (type == RSERVER_TYPE_POINTS) {
			auto points = ***REMOVED***::as<std::unique_ptr<PointCollection>>(result);
			is_sending = true;
			BinaryWriteBuffer response;
			response.write((char) -RSERVER_TYPE_POINTS);
			response.write(*points, true);
			stream.write(response);
		}
		else if (type == RSERVER_TYPE_STRING) {
			std::string output = Rcallbacks.getConsoleOutput();
			is_sending = true;
			BinaryWriteBuffer response;
			response.write((char) -RSERVER_TYPE_STRING);
			response.write(output, true);
			stream.write(response);
		}
		else if (type == RSERVER_TYPE_PLOT) {
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


void signal_handler(int signum) {
	Log::error("Caught signal %d, exiting", signum);
	exit(signum);
}


int main()
{
	Configuration::loadFromDefaultPaths();

	auto rserver_socket = Configuration::get("rserver.socket");

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

	Log::info("R is ready");

	// get rid of leftover sockets
	unlink(rserver_socket.c_str());

	int listen_fd;

	// create a socket
	listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (listen_fd < 0) {
		Log::error("socket() failed: %s", strerror(errno));
		exit(1);
	}

	/* bind socket */
	struct sockaddr_un server_addr;
	memset((void *) &server_addr, 0, sizeof(server_addr));
	server_addr.sun_family = AF_UNIX;
	strcpy(server_addr.sun_path, rserver_socket.c_str());
	if (bind(listen_fd, (sockaddr *) &server_addr, sizeof(server_addr)) < 0) {
		Log::error("bind() failed: %s", strerror(errno));
		exit(1);
	}

	chmod(rserver_socket.c_str(), 0777);


	std::map<pid_t, timespec> running_clients;

	Log::info("Socket started, listening..");
	// Start listening and fork()
	listen(listen_fd, 5);
	while (true) {
		// try to reap our children
		int status;
		pid_t exited_pid;
		while ((exited_pid = waitpid(-1, &status, WNOHANG)) > 0) {
			Log::info("Client %d no longer exists", (int) exited_pid);
			running_clients.erase(exited_pid);
		}
		// Kill all overdue children
		struct timespec current_t;
		clock_gettime(CLOCK_MONOTONIC, &current_t);

		for (auto it = running_clients.begin(); it != running_clients.end(); ) {
			auto timeout_t = it->second;
			if (cmpTimespec(timeout_t, current_t) < 0) {
				auto timeouted_pid = it->first;
				Log::info("Client %d gets killed due to timeout", (int) timeouted_pid);

				if (kill(timeouted_pid, SIGHUP) < 0) { // TODO: SIGKILL?
					Log::error("kill() failed: %s", strerror(errno));
				}
				// the postincrement of the iterator is important to avoid using an invalid iterator
				running_clients.erase(it++);
			}
			else {
				++it;
			}

		}

		// Wait for new connections. Do not wait longer than 5 seconds.
		struct pollfd pollfds[1];
		pollfds[0].fd = listen_fd;
		pollfds[0].events = POLLIN;

		int poll_res = poll(pollfds, /* count = */ 1, /* timeout in ms = */ 5000);
		if (poll_res < 0) {
			Log::error("poll() failed: %s", strerror(errno));
			exit(1);
		}
		if (poll_res == 0)
			continue;
		if ((pollfds[0].revents & POLLIN) == 0)
			continue;

		struct sockaddr_un client_addr;
		socklen_t client_addr_len = sizeof(client_addr);
		int client_fd = accept(listen_fd, (struct sockaddr *) &client_addr, &client_addr_len);
		if (client_fd < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				continue;
			Log::error("accept() failed: %s", strerror(errno));
			exit(1);
		}
		// fork
		pid_t pid = fork();
		if (pid < 0) {
			Log::error("fork() failed: %s", strerror(errno));
			exit(1);
		}

		if (pid == 0) {
			// This is the client
			// TODO: drop privileges!
			close(listen_fd);
			Log::info("New client starting");
			auto start_c = clock();
			struct timespec start_t;
			clock_gettime(CLOCK_MONOTONIC, &start_t);
			try {
				client(client_fd, R, *Rcallbacks);
			}
			catch (const std::exception &e) {
				Log::warn("Exception: %s", e.what());
			}
			auto end_c = clock();
			struct timespec end_t;
			clock_gettime(CLOCK_MONOTONIC, &end_t);

			double c = (double) (end_c - start_c) / CLOCKS_PER_SEC;
			double t = (double) (end_t.tv_sec - start_t.tv_sec) + (double) (end_t.tv_nsec - start_t.tv_nsec) / 1000000000;

			Log::info("Client finished, %.3fs real, %.3fs CPU", t, c);
			auto p = Profiler::get();
			for (auto &s : p) {
				Log::info("%s", s.c_str());
			}

			exit(0);
		}
		else {
			// This is the server
			close(client_fd);

			struct timespec timeout_t;
			clock_gettime(CLOCK_MONOTONIC, &timeout_t);
			timeout_t.tv_sec += TIMEOUT_SECONDS;
			running_clients[pid] = timeout_t;
		}
	}
}
