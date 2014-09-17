#include "raster/exceptions.h"
#include "util/socket.h"
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

#include <time.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>


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


std::unique_ptr<GenericRaster> query_raster_source(Socket &socket, int childidx, const QueryRectangle &rect) {
	Profiler::Profiler("requesting Raster");
	log("requesting raster %d with rect (%f,%f -> %f,%f)", childidx, rect.x1,rect.y1, rect.x2,rect.y2);
	socket.write((char) RSERVER_TYPE_RASTER);
	socket.write(childidx);
	socket.write(rect);

	auto raster = GenericRaster::fromSocket(socket);
	raster->setRepresentation(GenericRaster::Representation::CPU);
	return raster;
}

std::unique_ptr<PointCollection> query_points_source(Socket &socket, int childidx, const QueryRectangle &rect) {
	Profiler::Profiler("requesting Points");
	log("requesting points %d with rect (%f,%f -> %f,%f)", childidx, rect.x1,rect.y1, rect.x2,rect.y2);
	socket.write((char) RSERVER_TYPE_POINTS);
	socket.write(childidx);
	socket.write(rect);

	auto points = std::make_unique<PointCollection>(socket);
	return points;
}



void client(int sock_fd, ***REMOVED*** &R, ***REMOVED***Callbacks &Rcallbacks) {
	Socket socket(sock_fd, sock_fd);

	int magic;
	socket.read(&magic);
	if (magic != RSERVER_MAGIC_NUMBER)
		throw PlatformException("Client sent the wrong magic number");
	char type;
	socket.read(&type);
	log("Requested type: %d", type);
	std::string source;
	socket.read(&source);
	//printf("Requested source: %s\n", source.c_str());
	int rastersourcecount, pointssourcecount;
	socket.read(&rastersourcecount);
	socket.read(&pointssourcecount);
	log("Requested counts: %d %d", rastersourcecount, pointssourcecount);
	QueryRectangle qrect(socket);
	log("rectangle is rect (%f,%f -> %f,%f)", qrect.x1,qrect.y1, qrect.x2,qrect.y2);

	std::function<std::unique_ptr<GenericRaster>(int, const QueryRectangle &)> bound_raster_source = std::bind(query_raster_source, std::ref(socket), std::placeholders::_1, std::placeholders::_2);
	R["mapping.rastercount"] = rastersourcecount;
	R["mapping.loadRaster"] = ***REMOVED***::InternalFunction( bound_raster_source );

	std::function<std::unique_ptr<PointCollection>(int, const QueryRectangle &)> bound_points_source = std::bind(query_points_source, std::ref(socket), std::placeholders::_1, std::placeholders::_2);
	R["mapping.pointscount"] = pointssourcecount;
	R["mapping.loadPoints"] = ***REMOVED***::InternalFunction( bound_points_source );

	R["mapping.qrect"] = qrect;

	Profiler::start("running R script");
	auto result = R.parseEval(source);
	Profiler::stop("running R script");

	if (type == RSERVER_TYPE_RASTER) {
		auto raster = ***REMOVED***::as<std::unique_ptr<GenericRaster>>(result);
		socket.write((char) -RSERVER_TYPE_RASTER);
		socket.write(*raster);
	}
	else if (type == RSERVER_TYPE_STRING) {
		std::string output = Rcallbacks.getConsoleOutput();
		socket.write((char) -RSERVER_TYPE_STRING);
		socket.write(output);
	}
	else
		throw PlatformException("Unknown result type requested");
}


int main()
{
	// Initialize R environment
	***REMOVED***Callbacks *Rcallbacks = new ***REMOVED***Callbacks();
	***REMOVED*** R;
	R.set_callbacks( Rcallbacks );
	R.parseEvalQ("library(\"raster\")");
	Rcallbacks->resetConsoleOutput();

	printf("R is initialized and ready\n");


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


	printf("Socket started, listening..\n");
	// Start listening and fork()
	listen(listen_fd, 5);
	while (true) {
		struct sockaddr_un client_addr;
		socklen_t client_addr_len = sizeof(client_addr);
		int client_fd = accept(listen_fd, (struct sockaddr *) &client_addr, &client_addr_len);
		if (client_fd < 0) {
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
			client(client_fd, R, *Rcallbacks);
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
			// nothing more to do.
		}
	}
}
