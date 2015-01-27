#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "raster/rastersource.h"
#include "raster/colors.h"

#include "operators/operator.h"
#include "converters/converter.h"
#include "raster/profiler.h"


#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <iostream>
#include <fstream>
#include <exception>
#include <memory>

#include <json/json.h>

static char *program_name;
static void usage() {
		printf("Usage:\n");
		printf("%s convert <input_filename> <png_filename>\n", program_name);
		printf("%s createsource <epsg> <channel1_example> <channel2_example> ...\n", program_name);
		printf("%s loadsource <sourcename>\n", program_name);
		printf("%s import <sourcename> <filename> <filechannel> <sourcechannel> <timestamp>\n", program_name);
		printf("%s query <queryname> <png_filename>\n", program_name);
		printf("%s hash <queryname>\n", program_name);
		exit(5);
}


static void convert(int argc, char *argv[]) {
	if (argc < 3) {
		usage();
	}

	try {
		auto raster = GenericRaster::fromGDAL(argv[2], 1);
		GreyscaleColorizer c;
		raster->toPNG(argv[3], c);

	}
	catch (ImporterException &e) {
		printf("%s\n", e.what());
		exit(5);
	}
}

/**
 * Erstellt eine neue Rasterquelle basierend auf ein paar Beispielbildern
 */
static void createsource(int argc, char *argv[]) {
	std::unique_ptr<GenericRaster> raster;

	Json::Value root(Json::objectValue);

	Json::Value channels(Json::arrayValue);

	std::unique_ptr<LocalCRS> lcrs;

	epsg_t epsg = (epsg_t) atoi(argv[2]);

	for (int i=0;i<argc-3;i++) {
		try {
			raster = GenericRaster::fromGDAL(argv[i+3], 1, epsg);
		}
		catch (ImporterException &e) {
			printf("%s\n", e.what());
			exit(5);
		}

		if (i == 0) {
			lcrs.reset(new LocalCRS(raster->lcrs));

			Json::Value coords(Json::objectValue);
			Json::Value sizes(Json::arrayValue);
			Json::Value origins(Json::arrayValue);
			Json::Value scales(Json::arrayValue);
			for (int d=0;d<lcrs->dimensions;d++) {
				sizes[d] = lcrs->size[d];
				origins[d] = lcrs->origin[d];
				scales[d] = lcrs->scale[d];
			}
			coords["epsg"] = lcrs->epsg;
			coords["size"] = sizes;
			coords["origin"] = origins;
			coords["scale"] = scales;

			root["coords"] = coords;
		}
		else {
			if (!(*lcrs == raster->lcrs)) {
				printf("Channel %d has a different coordinate system than the first channel\n", i);
				exit(5);
			}
		}

		Json::Value channel(Json::objectValue);
		channel["datatype"] = GDALGetDataTypeName(raster->dd.datatype);
		channel["min"] = raster->dd.min;
		channel["max"] = raster->dd.max;
		if (raster->dd.has_no_data)
			channel["nodata"] = raster->dd.no_data;

		channels.append(channel);
	}

	root["channels"] = channels;

	Json::StyledWriter writer;
	std::string json = writer.write(root);

	printf("%s\n", json.c_str());
}


static void loadsource(int argc, char *argv[]) {
	if (argc < 3) {
		usage();
	}
	RasterSource *source = nullptr;
	try {
		source = RasterSourceManager::open(argv[2]);
	}
	catch (std::exception &e) {
		printf("Failure: %s\n", e.what());
	}
	RasterSourceManager::close(source);
}

// import <sourcename> <filename> <filechannel> <sourcechannel> <timestamp>
static void import(int argc, char *argv[]) {
	if (argc < 7) {
		usage();
	}
	RasterSource *source = nullptr;
	try {
		source = RasterSourceManager::open(argv[2], RasterSource::READ_WRITE);
		const char *filename = argv[3];
		int sourcechannel = atoi(argv[4]);
		int channelid = atoi(argv[5]);
		int timestamp = atoi(argv[6]);
		GenericRaster::Compression compression = GenericRaster::Compression::BZIP;
		if (argc > 7) {
			if (argv[7][0] == 'P')
				compression = GenericRaster::Compression::PREDICTED;
			else if (argv[7][0] == 'G')
				compression = GenericRaster::Compression::GZIP;
			else if (argv[7][0] == 'R')
				compression = GenericRaster::Compression::UNCOMPRESSED;
		}
		source->import(filename, sourcechannel, channelid, timestamp, compression);
	}
	catch (std::exception &e) {
		printf("Failure: %s\n", e.what());
	}
	RasterSourceManager::close(source);
}

static QueryRectangle qrect_from_json(Json::Value &root) {
	epsg_t epsg = (epsg_t) root.get("query_epsg", EPSG_WEBMERCATOR).asInt();
	double x1 = root.get("query_x1", -20037508).asDouble();
	double y1 = root.get("query_y1", -20037508).asDouble();
	double x2 = root.get("query_x2", 20037508).asDouble();
	double y2 = root.get("query_y2", 20037508).asDouble();
	int xres = root.get("query_xres", 1000).asInt();
	int yres = root.get("query_yres", 1000).asInt();
	int timestamp = root.get("starttime", 0).asInt();
	return QueryRectangle(timestamp, x1, y1, x2, y2, xres, yres, epsg);
}

static void runquery(int argc, char *argv[]) {
	if (argc < 4) {
		usage();
	}
	char *in_filename = argv[2];
	char *out_filename = argv[3];

	/*
	 * Step #1: open the query.json file and parse it
	 */
	std::ifstream file(in_filename);
	if (!file.is_open()) {
		printf("unable to open query file %s\n", in_filename);
		exit(5);
	}

	Json::Reader reader(Json::Features::strictMode());
	Json::Value root;
	if (!reader.parse(file, root)) {
		printf("unable to read json\n%s\n", reader.getFormattedErrorMessages().c_str());
		exit(5);
	}

	auto graph = GenericOperator::fromJSON(root["query"]);
	std::string result = root.get("query_result", "raster").asString();

	if (result == "raster") {
		QueryProfiler profiler;
		auto raster = graph->getCachedRaster(qrect_from_json(root), profiler);
		{
			Profiler::Profiler p("TO_GTIFF");
			raster->toGDAL(out_filename, "GTiff");
		}
	}
	else if (result == "points") {
		QueryProfiler profiler;
		auto points = graph->getCachedPoints(qrect_from_json(root), profiler);
		auto csv = points->toCSV();
		FILE *f = fopen(out_filename, "w");
		if (f) {
			fwrite(csv.c_str(), csv.length(), 1, f);
			fclose(f);
		}
	}
	else {
		printf("Unknown result type: %s\n", result.c_str());
		exit(5);
	}

	Profiler::print("\n");
}

static int testquery(int argc, char *argv[]) {
	if (argc < 3) {
		usage();
	}
	char *in_filename = argv[2];
	bool set_hash = false;
	if (argc >= 4 && argv[3][0] == 'S')
		set_hash = true;

	try {
		/*
		 * Step #1: open the query.json file and parse it
		 */
		std::ifstream file(in_filename);
		if (!file.is_open()) {
			printf("unable to open query file %s\n", in_filename);
			return 5;
		}

		Json::Reader reader(Json::Features::strictMode());
		Json::Value root;
		if (!reader.parse(file, root)) {
			printf("unable to read json\n%s\n", reader.getFormattedErrorMessages().c_str());
			return 5;
		}

		auto graph = GenericOperator::fromJSON(root["query"]);
		std::string result = root.get("query_result", "raster").asString();

		std::string real_hash;

		if (result == "raster") {
			QueryProfiler profiler;
			auto raster = graph->getCachedRaster(qrect_from_json(root), profiler);
			real_hash = raster->hash();
		}
		else if (result == "points") {
			QueryProfiler profiler;
			auto points = graph->getCachedPoints(qrect_from_json(root), profiler);
			real_hash = points->hash();
		}
		else {
			printf("Unknown result type: %s\n", result.c_str());
			return 5;
		}

		if (root.isMember("query_expected_hash")) {
			std::string expected_hash = root.get("query_expected_hash", "#").asString();
			printf("Expected: %s\nResult  : %s\n", expected_hash.c_str(), real_hash.c_str());

			if (expected_hash != real_hash) {
				printf("MISMATCH!!!\n");
				return 5;
			}
		}
		else if (set_hash) {
			root["query_expected_hash"] = real_hash;
			std::ofstream file(in_filename);
			file << root;
			file.close();
			printf("No hash in query file, added %s\n", real_hash.c_str());
			return 5;
		}
		else {
			printf("No hash in query file\n");
			return 5;
		}
	}
	catch (const std::exception &e) {
		printf("Exception: %s\n", e.what());
		return 5;
	}
	return 0;
}

int main(int argc, char *argv[]) {

	program_name = argv[0];

	if (argc < 2) {
		usage();
	}

	const char *command = argv[1];

	if (strcmp(command, "convert") == 0) {
		convert(argc, argv);
	}
	else if (strcmp(command, "createsource") == 0) {
		createsource(argc, argv);
	}
	else if (strcmp(command, "loadsource") == 0) {
		loadsource(argc, argv);
	}
	else if (strcmp(command, "import") == 0) {
		import(argc, argv);
	}
	else if (strcmp(command, "query") == 0) {
		runquery(argc, argv);
	}
	else if (strcmp(command, "testquery") == 0) {
		return testquery(argc, argv);
	}
	else {
		usage();
	}
	return 0;
}
