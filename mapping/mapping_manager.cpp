#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"
#include "datatypes/colorizer.h"
#include "rasterdb/rasterdb.h"
#include "raster/opencl.h"
#include "cache/manager.h"

#include "operators/operator.h"
#include "converters/converter.h"
#include "raster/profiler.h"
#include "util/binarystream.h"
#include "util/configuration.h"
#include "util/gdal.h"
#include "util/debug.h"

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
		printf("%s import <sourcename> <filename> <filechannel> <sourcechannel> <time_start> <duration> <compression>\n", program_name);
		printf("%s link <sourcename> <sourcechannel> <time_reference> <time_start> <duration>\n", program_name);
		printf("%s query <queryname> <png_filename>\n", program_name);
		printf("%s testquery <queryname>\n", program_name);
		printf("%s enumeratesources [verbose]\n", program_name);
		exit(5);
}

static void convert(int argc, char *argv[]) {
	if (argc < 3) {
		usage();
	}

	try {
		auto raster = GenericRaster::fromGDAL(argv[2], 1);
		auto c = Colorizer::fromUnit(raster->dd.unit);
		std::ofstream output(argv[3]);
		raster->toPNG(output, *c);
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

	std::unique_ptr<GDALCRS> lcrs;

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
			lcrs.reset(new GDALCRS(*raster));

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
			GDALCRS compare_crs(*raster);
			if (!(*lcrs == compare_crs)) {
				printf("Channel %d has a different coordinate system than the first channel\n", i);
				exit(5);
			}
		}

		Json::Value channel(Json::objectValue);
		channel["datatype"] = GDALGetDataTypeName(raster->dd.datatype);
		channel["unit"] = raster->dd.unit.toJsonObject();
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
	try {
		auto db = RasterDB::open(argv[2]);
	}
	catch (std::exception &e) {
		printf("Failure: %s\n", e.what());
	}
}

// import <sourcename> <filename> <filechannel> <sourcechannel> <time_start> <duration> <compression>
static void import(int argc, char *argv[]) {
	if (argc < 9) {
		usage();
	}
	try {
		auto db = RasterDB::open(argv[2], RasterDB::READ_WRITE);
		const char *filename = argv[3];
		int sourcechannel = atoi(argv[4]);
		int channelid = atoi(argv[5]);
		double time_start = atof(argv[6]);
		double duration = atof(argv[7]);
		RasterConverter::Compression compression = RasterConverter::Compression::GZIP;
		if (argv[8][0] == 'P')
			compression = RasterConverter::Compression::PREDICTED;
		else if (argv[8][0] == 'G')
			compression = RasterConverter::Compression::GZIP;
		else if (argv[8][0] == 'R')
			compression = RasterConverter::Compression::UNCOMPRESSED;
		db->import(filename, sourcechannel, channelid, time_start, time_start+duration, compression);
	}
	catch (std::exception &e) {
		printf("Failure: %s\n", e.what());
	}
}

// link <sourcename> <channel> <reference_time> <new_time_start> <new_duration>
static void link(int argc, char *argv[]) {
	if (argc < 7) {
		usage();
	}
	try {
		auto db = RasterDB::open(argv[2], RasterDB::READ_WRITE);
		int channelid = atoi(argv[3]);
		double time_reference = atof(argv[4]);
		double time_start = atof(argv[5]);
		double duration = atof(argv[6]);
		db->linkRaster(channelid, time_reference, time_start, time_start+duration);
	}
	catch (std::exception &e) {
		printf("Failure: %s\n", e.what());
	}
}

static SpatialReference sref_from_json(Json::Value &root, bool &flipx, bool &flipy){
	if(root.isMember("spatial_reference")){
		Json::Value& json = root["spatial_reference"];

		epsg_t epsg = epsgCodeFromSrsString(json.get("projection", "EPSG:4326").asString());

		double x1 = json.get("x1", -180).asDouble();
		double y1 = json.get("y1", -90).asDouble();
		double x2 = json.get("x2", 180).asDouble();
		double y2 = json.get("y2", 90).asDouble();

		return SpatialReference(epsg, x1, y1, x2, y2, flipx, flipy);
	}

	return SpatialReference::unreferenced();
}

static TemporalReference tref_from_json(Json::Value &root){
	if(root.isMember("temporal_reference")){
		Json::Value& json = root["temporal_reference"];

		std::string type = json.get("type", "UNIX").asString();

		timetype_t time_type;

		if(type == "UNIX")
			time_type = TIMETYPE_UNIX;
		else
			time_type = TIMETYPE_UNKNOWN;

		double start = json.get("start", 0).asDouble();
		if (json.isMember("end")) {
			double end = json.get("end", 0).asDouble();
			return TemporalReference(time_type, start, end);
		}

		return TemporalReference(time_type, start);
	}

	return TemporalReference::unreferenced();
}

static QueryResolution qres_from_json(Json::Value &root){
	if(root.isMember("resolution")){
		Json::Value& json = root["resolution"];

		std::string type = json.get("type", "none").asString();

		if(type == "pixels"){
			int x = json.get("x", 1000).asInt();
			int y = json.get("y", 1000).asInt();

			return QueryResolution::pixels(x, y);
		} else if(type == "none")
			return QueryResolution::none();
		else {
			fprintf(stderr, "invalid query resolution");
			exit(5);
		}


	}

	return QueryResolution::none();
}

static QueryRectangle qrect_from_json(Json::Value &root, bool &flipx, bool &flipy) {
	return QueryRectangle (
		sref_from_json(root, flipx, flipy),
		tref_from_json(root),
		qres_from_json(root)
	);
}

static QueryRectangle qrect_from_json(Json::Value &root) {
	bool flipx, flipy;
	return qrect_from_json(root, flipx, flipy);
}


static void runquery(int argc, char *argv[]) {
	if (argc < 3) {
		usage();
	}
	char *in_filename = argv[2];
	char *out_filename = nullptr;
	if (argc > 3)
		out_filename = argv[3];

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

	bool flipx, flipy;
	auto qrect = qrect_from_json(root, flipx, flipy);

	if (result == "raster") {
		QueryProfiler profiler;

		std::string queryModeParam = root.get("query_mode", "exact").asString();
		GenericOperator::RasterQM queryMode;

		if(queryModeParam == "exact")
			queryMode = GenericOperator::RasterQM::EXACT;
		else if(queryModeParam == "loose")
			queryMode = GenericOperator::RasterQM::LOOSE;
		else {
			fprintf(stderr, "invalid query mode");
			exit(5);
		}

		auto raster = graph->getCachedRaster(qrect, profiler, queryMode);
		printf("flip: %d %d\n", flipx, flipy);
		printf("QRect(%f,%f -> %f,%f)\n", qrect.x1, qrect.y1, qrect.x2, qrect.y2);
		if (flipx || flipy)
			raster = raster->flip(flipx, flipy);

		if (out_filename) {
			{
				Profiler::Profiler p("TO_GTIFF");
				raster->toGDAL((std::string(out_filename) + ".tif").c_str(), "GTiff", flipx, flipy);
			}
			{
				Profiler::Profiler p("TO_PNG");
				auto colors = Colorizer::fromUnit(raster->dd.unit);
				std::ofstream output(std::string(out_filename) + ".png");
				raster->toPNG(output, *colors);
			}
		}
		else
			printf("No output filename given, discarding result of size %d x %d\n", raster->width, raster->height);
	}
	else if (result == "points") {
		QueryProfiler profiler;
		auto points = graph->getCachedPointCollection(qrect, profiler);
		auto csv = points->toCSV();
		if (out_filename) {
			FILE *f = fopen(out_filename, "w");
			if (f) {
				fwrite(csv.c_str(), csv.length(), 1, f);
				fclose(f);
			}
		}
		else
			printf("No output filename given, discarding result\n");
	}
	else if (result == "lines") {
		QueryProfiler profiler;
		auto lines = graph->getCachedLineCollection(qrect, profiler);
		auto csv = lines->toCSV();
		if (out_filename) {
			FILE *f = fopen(out_filename, "w");
			if (f) {
				fwrite(csv.c_str(), csv.length(), 1, f);
				fclose(f);
			}
		}
		else
			printf("No output filename given, discarding result\n");
	}
	else if (result == "polygons") {
		QueryProfiler profiler;
		auto polygons = graph->getCachedPolygonCollection(qrect, profiler);
		auto csv = polygons->toCSV();
		if (out_filename) {
			FILE *f = fopen(out_filename, "w");
			if (f) {
				fwrite(csv.c_str(), csv.length(), 1, f);
				fclose(f);
			}
		}
		else
			printf("No output filename given, discarding result\n");
	}
	else if (result == "plot") {
			QueryProfiler profiler;
			auto plot = graph->getCachedPlot(qrect, profiler);
			auto json = plot->toJSON();
			if (out_filename) {
				FILE *f = fopen(out_filename, "w");
				if (f) {
					fwrite(json.c_str(), json.length(), 1, f);
					fclose(f);
				}
			}
			else
				printf("No output filename given, discarding result\n");
		}
	else {
		printf("Unknown result type: %s\n", result.c_str());
		exit(5);
	}

	auto msgs = get_debug_messages();
	for (auto &m : msgs) {
		printf("%s\n", m.c_str());
	}
}

static void testsemantic(GenericOperator *graph) {
	/*
	 * Make sure the graph's semantic id works
	 */
	try {
		auto semantic1 = graph->getSemanticId();
		std::unique_ptr<GenericOperator> graph2 = nullptr;
		try {
			graph2 = GenericOperator::fromJSON(semantic1);
		}
		catch (const std::exception &e) {
			printf("\nFAILED: semantic\nException parsing graph from semantic id: %s\n%s", e.what(), semantic1.c_str());
			return;
		}
		auto semantic2 = graph2->getSemanticId();
		if (semantic1 != semantic2) {
			printf("\nFAILED: semantic\nSemantic ID changes after reconstruction:\n%s\n%s\n", semantic1.c_str(), semantic2.c_str());
			return;
		}
	}
	catch (const std::exception &e) {
		printf("\nFAILED: semantic\nException during testsemantic: %s\n", e.what());
		return;
	}

	printf("\nPASSED: semantic\n");
	return;
}

template<typename T>
std::string getIPCHash(T &t) {
	BinaryWriteBuffer buf;
	buf << t;
	return buf.hash().asHex();
}

std::string getStringHash(const std::string &str) {
	SHA1 sha1;
	sha1.addBytes(str.data(), str.length());
	return sha1.digest().asHex();
}

// The return code is 0 for both success and failure, nonzero for any actual error (e.g. exception or crash)
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

		/*
		 * Step #2: test if the semantic ID of this query is working
		 */
		testsemantic(graph.get());

		/*
		 * Step #3: run the query and see if the results match
		 */
		std::string result = root.get("query_result", "raster").asString();
		std::string real_hash, real_hash2;

		bool flipx, flipy;
		auto qrect = qrect_from_json(root, flipx, flipy);
		if (result == "raster") {
			QueryProfiler profiler;

			std::string queryModeParam = root.get("query_mode", "exact").asString();
			GenericOperator::RasterQM queryMode;

			if(queryModeParam == "exact")
				queryMode = GenericOperator::RasterQM::EXACT;
			else if(queryModeParam == "loose")
				queryMode = GenericOperator::RasterQM::LOOSE;
			else {
				fprintf(stderr, "invalid query mode");
				return 5;
			}

			auto raster = graph->getCachedRaster(qrect, profiler, queryMode);
			if (flipx || flipy)
				raster = raster->flip(flipx, flipy);
			real_hash = getIPCHash(*raster);
			real_hash2 = getIPCHash(*raster->clone());
		}
		else if (result == "points") {
			QueryProfiler profiler;
			auto points = graph->getCachedPointCollection(qrect, profiler);
			real_hash = getIPCHash(*points);
			real_hash2 = getIPCHash(*points->clone());
		}
		else if (result == "lines") {
			QueryProfiler profiler;
			auto lines = graph->getCachedLineCollection(qrect, profiler);
			real_hash = getIPCHash(*lines);
			real_hash2 = getIPCHash(*lines->clone());
		}
		else if (result == "polygons") {
			QueryProfiler profiler;
			auto polygons = graph->getCachedPolygonCollection(qrect, profiler);
			real_hash = getIPCHash(*polygons);
			real_hash2 = getIPCHash(*polygons->clone());
		}
		else if (result == "plot") {
			QueryProfiler profiler;
			auto plot = graph->getCachedPlot(qrect, profiler);
			real_hash = getStringHash(plot->toJSON());
			real_hash2 = getStringHash(plot->clone()->toJSON());
		}
		else {
			printf("Unknown result type: %s\n", result.c_str());
			return 5;
		}

		if (real_hash != real_hash2) {
			printf("FAILED: hash\nHashes of result and its clone differ, probably a bug in clone():original: %s\ncopy:      %s\n", real_hash.c_str(), real_hash2.c_str());
			return 5;
		}

		if (root.isMember("query_expected_hash")) {
			std::string expected_hash = root.get("query_expected_hash", "#").asString();
			if (expected_hash == real_hash) {
				printf("\nPASSED: hash\n");
				return 0;
			}
			else {
				printf("\nFAILED: hash\nExpected : %s\nResult   : %s\n", expected_hash.c_str(), real_hash.c_str());
				return 0;
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
	return 10;
}


int main(int argc, char *argv[]) {

	program_name = argv[0];
	int returncode = 0;

	if (argc < 2) {
		usage();
	}

	Configuration::loadFromDefaultPaths();
	// FIXME
	NopCacheManager cm;
	CacheManager::init(&cm);

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
	else if (strcmp(command, "link") == 0) {
		link(argc, argv);
	}
	else if (strcmp(command, "query") == 0) {
		runquery(argc, argv);
	}
	else if (strcmp(command, "testquery") == 0) {
		returncode = testquery(argc, argv);
	}
	else if (strcmp(command, "enumeratesources") == 0) {
		bool verbose = false;
		if (argc > 2)
			verbose = true;
		auto names = RasterDB::getSourceNames();
		for (const auto &name : names) {
			printf("Source: %s\n", name.c_str());
			if (verbose) {
				printf("----------------------------------------------------------------------\n");
				auto json = RasterDB::getSourceDescription(name);
				printf("JSON: %s\n", json.c_str());
				printf("----------------------------------------------------------------------\n");
			}
		}
	}
	else if (strcmp(command, "msgcoord") == 0) {
		GDAL::CRSTransformer t(EPSG_LATLON, EPSG_GEOSMSG);
		auto f = [&] (double x, double y) -> void {
			double px = x, py = y;
			if (t.transform(px, py))
				printf("%f, %f -> %f, %f\n", x, y, px, py);
			else
				printf("%f, %f -> failed\n", x, y);
		};
		f(11, -16);
		f(36, -36);
		f(11, -36);
		f(36, -16);
	}
#ifndef MAPPING_NO_OPENCL
	else if (strcmp(command, "clinfo") == 0) {
		RasterOpenCL::init();
		auto mbs = RasterOpenCL::getMaxAllocSize();
		printf("maximum buffer size is %ud (%d MB)\n", mbs, mbs/1024/1024);
	}
#endif
	else {
		usage();
	}
#ifndef MAPPING_NO_OPENCL
	RasterOpenCL::free();
#endif
	return returncode;
}
