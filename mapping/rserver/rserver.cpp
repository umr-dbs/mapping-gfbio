#include "raster/exceptions.h"
#include "util/binarystream.h"
#include "rserver/rserver.h"

#include "raster/raster.h"
#include "raster/raster_priv.h"
#include "raster/pointcollection.h"
#include "plot/text.h"
#include "raster/profiler.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include <cstdlib>
#include <stdio.h>
#include <string.h> // memset()
#include <map>

#include <time.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <poll.h>
#include <signal.h>


const int TIMEOUT_SECONDS = 600;

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter" // silence the myriad of warnings in ***REMOVED*** headers
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

#include "rserver/***REMOVED***_wrapper.h" // definitions


#include "rserver/***REMOVED***_callbacks.h"

template<typename... Args>
void log(const char *str, Args&&... args) {
	fprintf(stderr, "%d: ", getpid());
	fprintf(stderr, str, args...);
	fprintf(stderr, "\n");
}

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

std::unique_ptr<GenericRaster> query_raster_source(BinaryStream &stream, int childidx, const QueryRectangle &rect) {
	Profiler::Profiler("requesting Raster");
	log("requesting raster %d with rect (%f,%f -> %f,%f)", childidx, rect.x1,rect.y1, rect.x2,rect.y2);
	stream.write((char) RSERVER_TYPE_RASTER);
	stream.write(childidx);
	stream.write(rect);

	auto raster = GenericRaster::fromStream(stream);
	raster->setRepresentation(GenericRaster::Representation::CPU);
	return raster;
}

***REMOVED***::NumericVector query_raster_source_as_array(BinaryStream &stream, int childidx, const QueryRectangle &rect) {
	Profiler::Profiler("requesting Raster");
	log("requesting raster %d with rect (%f,%f -> %f,%f)", childidx, rect.x1,rect.y1, rect.x2,rect.y2);
	stream.write((char) RSERVER_TYPE_RASTER);
	stream.write(childidx);
	stream.write(rect);

	auto raster = GenericRaster::fromStream(stream);
	raster->setRepresentation(GenericRaster::Representation::CPU);
	int width = raster->lcrs.size[0];
	int height = raster->lcrs.size[1];
	***REMOVED***::NumericVector pixels(raster->lcrs.getPixelCount());
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
	log("requesting points %d with rect (%f,%f -> %f,%f)", childidx, rect.x1,rect.y1, rect.x2,rect.y2);
	stream.write((char) RSERVER_TYPE_POINTS);
	stream.write(childidx);
	stream.write(rect);

	auto points = std::make_unique<PointCollection>(stream);
	return points;
}



void client(int sock_fd, ***REMOVED*** &R, ***REMOVED***Callbacks &Rcallbacks) {
	UnixSocket socket(sock_fd, sock_fd);
	BinaryStream &stream = socket;

	int magic;
	stream.read(&magic);
	if (magic != RSERVER_MAGIC_NUMBER)
		throw PlatformException("Client sent the wrong magic number");
	char type;
	stream.read(&type);
	log("Requested type: %d", type);
	std::string source;
	stream.read(&source);
	//printf("Requested source: %s\n", source.c_str());
	int rastersourcecount, pointssourcecount;
	stream.read(&rastersourcecount);
	stream.read(&pointssourcecount);
	log("Requested counts: %d %d", rastersourcecount, pointssourcecount);
	QueryRectangle qrect(stream);
	log("rectangle is rect (%f,%f -> %f,%f)", qrect.x1,qrect.y1, qrect.x2,qrect.y2);

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

	std::string delimiter = "\n\n";
	size_t start = 0;
	size_t end = 0;
	while (true) {
		end = source.find(delimiter, start);
		if (end == std::string::npos)
			break;
	    std::string line = source.substr(start, end-start);
	    start = end+delimiter.length();
	    log("src: %s", line.c_str());
	    R.parseEval(line);
	}
	std::string lastline = source.substr(start);
	log("src: %s", lastline.c_str());
	auto result = R.parseEval(lastline);
	Profiler::stop("running R script");

	if (type == RSERVER_TYPE_RASTER) {
		auto raster = ***REMOVED***::as<std::unique_ptr<GenericRaster>>(result);
		stream.write((char) -RSERVER_TYPE_RASTER);
		stream.write(*raster);
	}
	else if (type == RSERVER_TYPE_STRING) {
		std::string output = Rcallbacks.getConsoleOutput();
		stream.write((char) -RSERVER_TYPE_STRING);
		stream.write(output);
	}
	else
		throw PlatformException("Unknown result type requested");
}


void signal_handler(int signum) {
	log("Caught signal %d, exiting", signum);
	exit(signum);
}


int main()
{
	// Signal handlers
	int signals[] = {SIGHUP, SIGINT, 0};
	for (int i=0;signals[i];i++) {
		if (signal(signals[i], signal_handler) == SIG_ERR) {
			perror("Cannot install signal handler");
			exit(1);
		}
		else
			printf("Signal handler for %d installed\n", signals[i]);
	}
	signal(SIGPIPE, SIG_IGN);

	// Initialize R environment
	***REMOVED***Callbacks *Rcallbacks = new ***REMOVED***Callbacks();
	printf("...loading R\n");
	***REMOVED*** R;
	R.setVerbose(true);
	R.set_callbacks( Rcallbacks );

	printf("...loading packages\n");

	R.parseEvalQ("library(\"raster\")");
	R.parseEvalQ("library(\"caret\")");
	R.parseEvalQ("library(\"randomForest\")");
	Rcallbacks->resetConsoleOutput();

	printf("R is ready\n");


	// get rid of leftover sockets
	unlink(rserver_socket_address);

	int listen_fd;

	// create a socket
	listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (listen_fd < 0) {
		perror("socket() failed");
		exit(1);
	}

	/* bind socket */
	struct sockaddr_un server_addr;
	memset((void *) &server_addr, 0, sizeof(server_addr));
	server_addr.sun_family = AF_UNIX;
	strcpy(server_addr.sun_path, rserver_socket_address);
	if (bind(listen_fd, (sockaddr *) &server_addr, sizeof(server_addr)) < 0) {
		 perror("bind() failed");
		 exit(1);
	}

	chmod(rserver_socket_address, 0777);


	std::map<pid_t, timespec> running_clients;

	printf("Socket started, listening..\n");
	// Start listening and fork()
	listen(listen_fd, 5);
	while (true) {
		// try to reap our children
		int status;
		pid_t exited_pid;
		while ((exited_pid = waitpid(-1, &status, WNOHANG)) > 0) {
			log("Client %d no longer exists", (int) exited_pid);
			running_clients.erase(exited_pid);
		}
		// Kill all overdue children
		struct timespec current_t;
		clock_gettime(CLOCK_MONOTONIC, &current_t);

		for (auto it = running_clients.begin(); it != running_clients.end(); ) {
			auto timeout_t = it->second;
			if (cmpTimespec(timeout_t, current_t) < 0) {
				auto timeouted_pid = it->first;
				log("Client %d gets killed due to timeout", (int) timeouted_pid);

				if (kill(timeouted_pid, SIGHUP) < 0) { // TODO: SIGKILL?
					perror("kill() failed");
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
			perror("poll() failed");
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
			perror("accept() failed");
			exit(1);
		}
		// fork
		pid_t pid = fork();
		if (pid < 0) {
			perror("fork() failed");
			exit(1);
		}

		if (pid == 0) {
			// This is the client
			// TODO: drop privileges!
			close(listen_fd);
			log("Client starting");
			auto start_c = clock();
			struct timespec start_t;
			clock_gettime(CLOCK_MONOTONIC, &start_t);
			try {
				client(client_fd, R, *Rcallbacks);
			}
			catch (const std::exception &e) {
				log("Exception: %s", e.what());
			}
			auto end_c = clock();
			struct timespec end_t;
			clock_gettime(CLOCK_MONOTONIC, &end_t);

			double c = (double) (end_c - start_c) / CLOCKS_PER_SEC;
			double t = (double) (end_t.tv_sec - start_t.tv_sec) + (double) (end_t.tv_nsec - start_t.tv_nsec) / 1000000000;

			log("Client finished, %.3fs real, %.3fs CPU", t, c);
			auto p = Profiler::get();
			for (auto &s : p) {
				log("%s", s.c_str());
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
