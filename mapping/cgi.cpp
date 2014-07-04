#include "raster/raster.h"
#include "raster/pointcollection.h"
#include "raster/histogram.h"
#include "raster/colors.h"
#include "raster/profiler.h"
#include "operators/operator.h"

//#include <cstdlib>
#include <cstdio>
#include <cmath> // isnan
#include <string>
#include <fstream>
#include <sstream>
#include <exception>
#include <algorithm>
#include <map>
#include <memory>
#include <tuple>

#include <uriparser/Uri.h>
#include <json/json.h>


[[noreturn]] static void abort(const char *msg) {
	printf("Content-type: text/plain\r\n\r\n%s", msg);
	exit(5);
}

static void abort(const std::string &msg) {
	abort(msg.c_str());
}

static void printInfo(int argc, char *argv[], const char *query_string) {
	printf("Content-type: text/plain\r\n\r\n");

	printf("argc: %d\n", argc);
	for (int i=0;i<argc;i++) {
		printf("argv[%d]: %s\n", i, argv[i]);
	}
	if (query_string)
		printf("Query String: %s\n", query_string);
	else
		printf("No query string\n");
}

static char hexvalue(char c) {
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	return 0;
}

static std::string urldecode(const char *string) {
	const int len = strlen(string);

	std::vector<char> buffer;
	buffer.reserve(len);

	int pos = 0;
	while (pos < len) {
		const char c = string[pos];
		if (c == '%' && pos + 2 < len) {
			char out = 16 * hexvalue(string[pos+1]) + hexvalue(string[pos+2]);
			buffer.push_back(out);
			pos+=3;
		}
		else {
			buffer.push_back(c);
			pos++;
		}
	}

	return std::string(buffer.begin(), buffer.end());
}

static std::map<std::string, std::string> parseQueryString(const char *query_string) {
	std::map<std::string, std::string> query_params;

	UriQueryListA *queryList;
	int itemCount;

	if (uriDissectQueryMallocA(&queryList, &itemCount, query_string, &query_string[strlen(query_string)]) != URI_SUCCESS)
		abort("Malformed query string");

	UriQueryListA *item = queryList;
	while (item) {
		std::string key(item->key);
		std::transform(key.begin(), key.end(), key.begin(), ::tolower);
		//std::string value(item->value);
		query_params[key] = urldecode(item->value);
		//query_params.insert( std::make_pair( std::string(item->key), std::string(item->value) ) );
		item = item->next;
	}

	uriFreeQueryListA(queryList);

	return query_params;
}


std::tuple<GenericOperator *, int, std::string>loadQuery(const char *in_filename) {
	/*
	 * Step #1: open the query.json file and parse it
	 */
	std::ostringstream complete_in_filename;
	complete_in_filename << "queries/" << in_filename  << ".json";
	std::ifstream file(complete_in_filename.str());
	if (!file.is_open()) {
		throw ArgumentException(std::string("unable to open query file: ") + complete_in_filename.str());
	}

	Json::Reader reader(Json::Features::strictMode());
	Json::Value root;
	if (!reader.parse(file, root)) {
		abort("unable to parse json");
	}

	int timestamp = root.get("starttime", 0).asInt();

	GenericOperator *graph = GenericOperator::fromJSON(root["query"]);

	return std::make_tuple(graph, timestamp, root.get("colorizer", "").asString());
}

std::tuple<GenericOperator *, int, std::string>parseQuery(const char *query) {
	std::istringstream iss(query);
	Json::Reader reader(Json::Features::strictMode());
	Json::Value root;
	if (!reader.parse(iss, root)) {
		abort("unable to parse json");
	}

	int timestamp = 42;

	GenericOperator *graph = GenericOperator::fromJSON(root);

	return std::make_tuple(graph, timestamp, "");
}

void outputImage(GenericRaster *raster, bool flipx = false, bool flipy = false, const std::string &colors = "") {

	std::unique_ptr<Colorizer> colorizer;
	if (colors == "hsv")
		colorizer.reset(new HSVColorizer());
	else
		colorizer.reset(new GreyscaleColorizer());

#if 1
	printf("Content-type: image/png\r\n\r\n");

	raster->toPNG(nullptr, *colorizer, flipx, flipy); //"/tmp/xyz.tmp.png");
#else
	printf("Content-type: image/jpeg\r\n\r\n");

	raster->toJPEG(nullptr, *colorizer, flipx, flipy); //"/tmp/xyz.tmp.jpg");
#endif
}

void outputImageByQuery(const char *in_filename) {
	auto p = loadQuery(in_filename);

	GenericOperator *graph = std::get<0>(p);
	std::unique_ptr<GenericOperator> graph_guard(graph);

	int timestamp = std::get<1>(p);
	std::string colors = std::get<2>(p);

	GenericRaster *raster = graph->getRaster(QueryRectangle(timestamp, -20037508, 20037508, 20037508, -20037508, 1024, 1024, EPSG_WEBMERCATOR));
	std::unique_ptr<GenericRaster> raster_guard(raster);

#if RASTER_DO_PROFILE && false
	/*
	// SAVE_PNG8:   0.052097
	// SAVE_PNG32:  0.249503
	// SAVE_JPEG8:  0.021444 (90%)
	// SAVE_JPEG32: 0.060772 (90%)
	// SAVE_JPEG8:  0.021920 (100%)
	// SAVE_JPEG32: 0.060187 (100%)

	Sizes:
	JPEG8:  200526 (100%)
	PNG8:   159504
	JPEG8:  124698 (95%)
	JPEG8:   92284 (90%)

	PNG32:  366925
	JPEG32: 308065 (100%)
	JPEG32: 168333 (95%)
	JPEG32: 120703 (90%)
	*/

	bool flipx = false;
	bool flipy = false;
	GreyscaleColorizer c1;
	HSVColorizer c2;
	{
		Profiler::Profiler p("SAVE_PNG8");
		raster->toPNG("/tmp/testimage1.png", c1, flipx, flipy);
	}
	{
		Profiler::Profiler p("SAVE_PNG32");
		raster->toPNG("/tmp/testimage2.png", c2, flipx, flipy);
	}
	{
		Profiler::Profiler p("SAVE_JPEG8");
		raster->toJPEG("/tmp/testimage1.jpg", c1, flipx, flipy);
	}
	{
		Profiler::Profiler p("SAVE_JPEG32");
		raster->toJPEG("/tmp/testimage2.jpg", c2, flipx, flipy);
	}
#endif
#if RASTER_DO_PROFILE
	printf("Profiling-header: ");
	Profiler::print();
	printf("\r\n");
#endif

	outputImage(raster_guard.get(), false, false, colors);
}


void outputPointCollection(PointCollection *points) {
	printf("Content-type: application/json\r\n\r\n%s", points->toGeoJSON().c_str());
}


int main() {
	//printf("Content-type: text/plain\r\n\r\nDebugging:\n");
	try {
		const char *query_string = getenv("QUERY_STRING");
		if (!query_string) {
			//query_string = "SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image%2Fpng8&TRANSPARENT=true&LAYERS=load&TILED=true&STYLES=dem&CRS=EPSG%3A3857&WIDTH=2571&HEIGHT=1350&BBOX=-8412792.231579678%2C-5618420.447247234%2C16741716.532732407%2C7589898.040431223";
			abort("No query string given");
		}

		//printInfo();return 0;
		std::map<std::string, std::string> params = parseQueryString(query_string);

		// direct loading of a query (obsolete?)
		if (params.count("query") > 0) {
			auto p = parseQuery(params["query"].c_str());

			GenericOperator *graph = std::get<0>(p);
			std::unique_ptr<GenericOperator> graph_guard(graph);

			int timestamp = std::get<1>(p);
			//std::string colors = std::get<2>(p);
			std::string colorizer;
			if (params.count("colors") > 0)
				colorizer = params["colors"];

			GenericRaster *raster = graph->getRaster(QueryRectangle(timestamp, -20037508, 20037508, 20037508, -20037508, 1024, 1024, EPSG_WEBMERCATOR));
			std::unique_ptr<GenericRaster> raster_guard(raster);

#if RASTER_DO_PROFILE
			printf("Profiling-header: ");
			Profiler::print();
			printf("\r\n");
#endif
			outputImage(raster_guard.get(), false, false, colorizer);
			return 0;
		}

		// PointCollection as GeoJSON
		if (params.count("pointquery") > 0) {
			auto p = parseQuery(params["pointquery"].c_str());

			GenericOperator *graph = std::get<0>(p);
			std::unique_ptr<GenericOperator> graph_guard(graph);

			int timestamp = std::get<1>(p);

			PointCollection *points = graph->getPoints(QueryRectangle(timestamp, -20037508, 20037508, 20037508, -20037508, 1024, 1024, EPSG_WEBMERCATOR));
			std::unique_ptr<PointCollection> points_guard(points);

#if RASTER_DO_PROFILE
			printf("Profiling-header: ");
			Profiler::print();
			printf("\r\n");
#endif
			outputPointCollection(points);
			return 0;
		}


		// WMS-Requests
		if (params.count("service") > 0 && params["service"] == "WMS") {
			std::string request = params["request"];
			// GetCapabilities
			if (request == "GetCapabilities") {

			}
			// GetMap
			else if (request == "GetMap") {
				std::string version = params["version"];
				if (params["version"] != "1.3.0")
					abort("Invalid version");

				// Wir ignorieren:
				// format
				// transparent
				// &CRS=EPSG%3A3857

				// Unbekannt:
				// &STYLES=dem


				//if (params["tiled"] != "true")
				//	abort("only tiled for now");

				std::string bbox_str = params["bbox"]; // &BBOX=0,0,10018754.171394622,10018754.171394622
				double bbox[4];

				{
					std::string delimiters = " ,";
					size_t current, next = -1;
					int element = 0;
					do {
					  current = next + 1;
					  next = bbox_str.find_first_of(delimiters, current);
					  double value = std::stod( bbox_str.substr(current, next - current) );
					  if (isnan(value))
						  abort("BBOX value is NaN");
					  bbox[element++] = value;
					} while (element < 4 && next != std::string::npos);

					if (element != 4)
						abort("BBOX does not contain 4 doubles");
				}

				// WebMercator, http://www.easywms.com/easywms/?q=en/node/3592
				                //    minx          miny         maxx         maxy
				double extent[4] {-20037508.34, -20037508.34, 20037508.34, 20037508.34};
				double bbox_normalized[4];
				for (int i=0;i<4;i+=2) {
					bbox_normalized[i  ] = (bbox[i  ] - extent[0]) / (extent[2]-extent[0]);
					bbox_normalized[i+1] = (bbox[i+1] - extent[1]) / (extent[3]-extent[1]);
				}

				// Koordinaten können leicht ausserhalb liegen, z.B.
				// 20037508.342789, 20037508.342789
				for (int i=0;i<4;i++) {
					if (bbox_normalized[i] < 0.0 && bbox_normalized[i] > -0.001)
						bbox_normalized[i] = 0.0;
					else if (bbox_normalized[i] > 1.0 && bbox_normalized[i] < 1.001)
						bbox_normalized[i] = 1.0;
				}

				for (int i=0;i<4;i++) {
					if (bbox_normalized[i] < 0.0 || bbox_normalized[i] > 1.0) {
						printf("Content-type: text/plain\r\n\r\n");
						printf("extent: (%f, %f) -> (%f, %f)\n", extent[0], extent[1], extent[2], extent[3]);
						printf("   raw: (%f, %f) -> (%f, %f)\n", bbox[0], bbox[1], bbox[2], bbox[3]);
						printf("normal: (%10f, %10f) -> (%10f, %10f)\n", bbox_normalized[0], bbox_normalized[1], bbox_normalized[2], bbox_normalized[3]);
						abort("bbox outside of extent");
					}
				}

				//bbox_normalized[1] = 1.0 - bbox_normalized[1];
				//bbox_normalized[3] = 1.0 - bbox_normalized[3];

				int output_width = atoi(params["width"].c_str());
				int output_height = atoi(params["height"].c_str());
				if (output_width <= 0 || output_height <= 0) {
					abort("output_width not valid");
				}

				const char *query_name = params["layers"].c_str();

				decltype(loadQuery(nullptr)) p;
				p = parseQuery(query_name);
				//p = loadQuery(query_name);

				GenericOperator *graph = std::get<0>(p);
				std::unique_ptr<GenericOperator> graph_guard(graph);

				int timestamp = std::get<1>(p);
				//std::string colors = std::get<2>(p);
				std::string colorizer;
				if (params.count("colors") > 0)
					colorizer = params["colors"];

				epsg_t epsg = EPSG_WEBMERCATOR;
				if (params.count("crs") > 0) {
					std::string crs = params["crs"];
					if (crs.compare(0,5,"EPSG:") == 0) {
						epsg = atoi(crs.substr(5, std::string::npos).c_str());
					}
				}


				std::string format("image/png");
				if (params.count("format") > 0) {
					format = params["format"];
				}

				QueryRectangle qrect(timestamp, bbox[0], bbox[1], bbox[2], bbox[3], output_width, output_height, epsg);

				if (format == "application/json") {
					Histogram *histogram = graph->getHistogram(qrect);
					std::unique_ptr<Histogram> histogram_guard(histogram);

					printf("content-type: application/json\r\n\r\n");
					histogram->print();
				}
				else {
					GenericRaster *raster = graph->getRaster(qrect);
					std::unique_ptr<GenericRaster> result_raster(raster);

					if (result_raster->lcrs.size[0] != (uint32_t) output_width || result_raster->lcrs.size[1] != (uint32_t) output_height) {
						result_raster.reset( result_raster->scale(output_width, output_height) );
					}

					bool flipx = (bbox[2] > bbox[0]) != (result_raster->lcrs.scale[0] > 0);
					bool flipy = (bbox[3] > bbox[1]) == (result_raster->lcrs.scale[1] > 0);

#if RASTER_DO_PROFILE
					printf("Profiling-header: ");
					Profiler::print();
					printf("\r\n");
#endif
					outputImage(result_raster.get(), flipx, flipy, colorizer);
				}
				// cut into pieces



				/*
 	 	 	 	 ?SERVICE=WMS
 	 	 	 	 &VERSION=1.3.0
 	 	 	 	 &REQUEST=GetMap
 	 	 	 	 &FORMAT=image%2Fpng8
 	 	 	 	 &TRANSPARENT=true
 	 	 	 	 &LAYERS=elevation%3Asrtm_41_90m
 	 	 	 	 &TILED=true
 	 	 	 	 &STYLES=dem
 	 	 	 	 &WIDTH=256
 	 	 	 	 &HEIGHT=256
 	 	 	 	 &CRS=EPSG%3A3857
 	 	 	 	 &BBOX=0%2C0%2C10018754.171394622%2C10018754.171394622
 	 	 	 	*/
				/*
				addLayer(LayerType.WMS, {
					layer : {
						url : 'http://dbsvm.mathematik.uni-marburg.de:9833/geoserver/elevation/wms',
						params : {
							'LAYERS' : 'elevation:srtm_41_90m',
							'TILED' : true,
							'FORMAT' : 'image/png8',
							'STYLES' : 'dem'
						},
						serverType : 'geoserver'
					},
					title : "SRTM"
				});
				 */
			}
			// GetFeatureInfo (optional)
			else if (request == "GetFeatureInfo") {


			}
			return 0;
		}

		abort(std::string("Unknown request: ") + params["request"]);
	}
	catch (std::exception &e) {
		abort(std::string("Internal error, exception: ") + e.what());
		//printf("Caught exception: %s\n", e.what());
	}

}
