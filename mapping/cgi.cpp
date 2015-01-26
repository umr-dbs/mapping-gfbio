#include "datatypes/raster.h"
#include "datatypes/raster/raster_priv.h"
#include "datatypes/pointcollection.h"
#include "datatypes/geometry.h"
#include "datatypes/plot.h"
#include "raster/colors.h"
#include "raster/profiler.h"
#include "operators/operator.h"
#include "util/debug.h"

#include <cstdio>
#include <cstdlib>
#include <cmath> // isnan
#include <string>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <map>
#include <memory>
#include <string>

#include <iomanip>
#include <cctype>

#include <uriparser/Uri.h>
#include <json/json.h>

#include "pointvisualization/CircleClusteringQuadTree.h"

/*
A few benchmarks:
SAVE_PNG8:   0.052097
SAVE_PNG32:  0.249503
SAVE_JPEG8:  0.021444 (90%)
SAVE_JPEG32: 0.060772 (90%)
SAVE_JPEG8:  0.021920 (100%)
SAVE_JPEG32: 0.060187 (100%)

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

		std::string value = urldecode(item->value);
		if (key == "subset" || key == "size") {
			auto pos = value.find(',');
			if (pos != std::string::npos) {
				key = key + '_' + value.substr(0, pos);
				value = value.substr(pos+1);
			}
		}
		//std::string value(item->value);
		query_params[key] = value;
		//query_params.insert( std::make_pair( std::string(item->key), std::string(item->value) ) );
		item = item->next;
	}

	uriFreeQueryListA(queryList);

	return query_params;
}


void outputImage(GenericRaster *raster, bool flipx = false, bool flipy = false, const std::string &colors = "", Raster2D<uint8_t> *overlay = nullptr) {
	auto colorizer = Colorizer::make(colors);
	print_debug_header();
#if 1
	printf("Content-type: image/png\r\n\r\n");

	raster->toPNG(nullptr, *colorizer, flipx, flipy, overlay); //"/tmp/xyz.tmp.png");
#else
	printf("Content-type: image/jpeg\r\n\r\n");

	raster->toJPEG(nullptr, *colorizer, flipx, flipy); //"/tmp/xyz.tmp.jpg");
#endif
}


void outputPointCollection(PointCollection *points, bool displayMetadata = false) {
	print_debug_header();
	printf("Content-type: application/json\r\n\r\n%s", points->toGeoJSON(displayMetadata).c_str());
}

void outputPointCollectionCSV(PointCollection *points) {
	print_debug_header();
	printf("Content-type: text/csv\r\nContent-Disposition: attachment; filename=\"export.csv\"\r\n\r\n%s", points->toCSV().c_str());
}

void outputGeometry(GenericGeometry *geometry) {
	print_debug_header();
	printf("Content-type: application/json\r\n\r\n%s", geometry->toGeoJSON().c_str());
}

bool to_bool(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    std::istringstream is(str);
    bool b;
    is >> std::boolalpha >> b;
    return b;
}

void parseBBOX(double *bbox, const std::string bbox_str, epsg_t epsg = EPSG_WEBMERCATOR, bool allow_infinite = false) {
	// &BBOX=0,0,10018754.171394622,10018754.171394622
	for(int i=0;i<4;i++)
		bbox[i] = NAN;

	// Figure out if we know the extent of the CRS
	// WebMercator, http://www.easywms.com/easywms/?q=en/node/3592
	//                               minx          miny         maxx         maxy
	double extent_webmercator[4] {-20037508.34 , -20037508.34 , 20037508.34 , 20037508.34};
	double extent_latlon[4]      {     -180    ,       -90    ,      180    ,       90   };
	double extent_msg[4]         { -5568748.276,  -5568748.276, 5568748.276,  5568748.276};

	double *extent = nullptr;
	if (epsg == EPSG_WEBMERCATOR)
		extent = extent_webmercator;
	else if (epsg == EPSG_LATLON)
		extent = extent_latlon;
	else if (epsg == EPSG_GEOSMSG)
		extent = extent_msg;


	std::string delimiters = " ,";
	size_t current, next = -1;
	int element = 0;
	do {
		current = next + 1;
		next = bbox_str.find_first_of(delimiters, current);
		std::string stringValue = bbox_str.substr(current, next - current);
		double value = 0;

		if (stringValue == "Infinity") {
			if (!allow_infinite)
				throw ArgumentException("cannot process BBOX with Infinity");
			if (!extent)
				throw ArgumentException("cannot process BBOX with Infinity and unknown CRS");
			value = std::max(extent[element], extent[(element+2)%4]);
		}
		else if (stringValue == "-Infinity") {
			if (!allow_infinite)
				throw ArgumentException("cannot process BBOX with Infinity");
			if (!extent)
				throw ArgumentException("cannot process BBOX with Infinity and unknown CRS");
			value = std::min(extent[element], extent[(element+2)%4]);
		}
		else {
			value = std::stod(stringValue);
			if (!std::isfinite(value))
				throw ArgumentException("BBOX contains entry that is not a finite number");
		}

		bbox[element++] = value;
	} while (element < 4 && next != std::string::npos);

	if (element != 4)
		throw ArgumentException("Could not parse BBOX parameter");

	/*
	 * OpenLayers insists on sending latitude in x and longitude in y.
	 * The MAPPING code (including gdal's projection classes) don't agree: east/west should be in x.
	 * The simple solution is to swap the x and y coordinates.
	 */
	if (epsg == EPSG_LATLON) {
		std::swap(bbox[0], bbox[1]);
		std::swap(bbox[2], bbox[3]);
	}

	// If no extent is known, just trust the client.
	if (extent) {
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
			if (bbox_normalized[i] < 0.0 || bbox_normalized[i] > 1.0)
				throw ArgumentException("BBOX exceeds extent");
		}
	}

	//bbox_normalized[1] = 1.0 - bbox_normalized[1];
	//bbox_normalized[3] = 1.0 - bbox_normalized[3];
}


void processWFS(std::map<std::string, std::string> &params, epsg_t query_epsg, time_t timestamp) {
	if(params["request"] == "GetFeature") {
		std::string version = params["version"];
		if (version != "2.0.0")
			abort("Invalid version");

		int output_width = std::stoi(params["width"]);
		int output_height = std::stoi(params["height"]);
		if (output_width <= 0 || output_height <= 0) {
			abort("output_width not valid");
		}

		double bbox[4];
		parseBBOX(bbox, params.at("bbox"), query_epsg, true);


		auto graph = GenericOperator::fromJSON(params["layers"]);

		QueryProfiler profiler;
		auto points = graph->getCachedPoints(QueryRectangle(timestamp, bbox[0], bbox[1], bbox[2], bbox[3], output_width, output_height, query_epsg), profiler);

		if(to_bool(params["clustered"]) == true) {
			auto clusteredPoints = std::make_unique<PointCollection>(query_epsg);

			auto x1 = bbox[0];
			auto x2 = bbox[2];
			auto y1 = bbox[1];
			auto y2 = bbox[3];
			auto xres = output_width;
			auto yres = output_height;
			pv::CircleClusteringQuadTree clusterer(pv::BoundingBox(
															pv::Coordinate((x2 + x1) / (2 * xres), (y2 + y1) / (2 * yres)),
															pv::Dimension((x2 - x1) / (2 * xres), (y2 - y1) / (2 * yres)),
														1), 1);
			for (Point& pointOld : points->collection) {
				clusterer.insert(std::make_shared<pv::Circle>(pv::Coordinate(pointOld.x / xres, pointOld.y / yres), 5, 1));
			}

			auto circles = clusterer.getCircles();
			clusteredPoints->local_md_value.addVector("radius", circles.size());
			clusteredPoints->local_md_value.addVector("numberOfPoints", circles.size());
			for (auto& circle : circles) {
				size_t idx = clusteredPoints->addPoint(circle->getX() * xres, circle->getY() * yres);
				clusteredPoints->local_md_value.set(idx, "radius", circle->getRadius());
				clusteredPoints->local_md_value.set(idx, "numberOfPoints", circle->getNumberOfPoints());
			}

			points.swap(clusteredPoints);
		}

		outputPointCollection(points.get(), true);
	}
}


std::pair<std::string, std::string> getCrsInformationFromOGCUri(std::string openGisUri){
	size_t beforeAutorityId = openGisUri.find("crs")+3;
	size_t behindAutorityId = openGisUri.find_first_of("/",beforeAutorityId+1);
	std::string authorityId = openGisUri.substr(beforeAutorityId+1, behindAutorityId-beforeAutorityId-1);
	std::cerr<<"getCrsInformationFromOGCUri openGisUri: "<<openGisUri<<" beforeAutorityId: "<<beforeAutorityId<<" behindAutorityId: "<<behindAutorityId<<" authorityId: "<<authorityId<<std::endl;

	//get the crsID
	size_t beforeCrsCode = openGisUri.find_last_of("/");
	size_t behindCrsCode = openGisUri.find_first_of("(", beforeCrsCode);
	if(behindCrsCode == std::string::npos)
		behindCrsCode = openGisUri.length();

	std::string crsCode = openGisUri.substr(beforeCrsCode+1, behindCrsCode-beforeCrsCode-1);
	std::cerr<<"getCrsInformationFromOGCUri openGisUri: "<<openGisUri<<" beforeCrsCode: "<<beforeCrsCode<<" behindCrsCode: "<<behindCrsCode<<" crsCode: "<<crsCode<<std::endl;

	//
	return (std::pair<std::string, std::string>{"EPSG",crsCode});
}

std::pair<double, double> getWfsParameterRangeDouble(std::string wfsParameterString){
	std::pair<double, double> resultPair;

	size_t rangeStart = wfsParameterString.find_first_of("(");
	size_t rangeEnd = wfsParameterString.find_last_of(")");
	size_t rangeSeperator = wfsParameterString.find_first_of(",", rangeStart);
	size_t firstEnd = (rangeSeperator == std::string::npos) ? rangeEnd : rangeSeperator;

	resultPair.first = std::stod(wfsParameterString.substr(rangeStart+1, firstEnd-rangeStart -1));

	if(rangeSeperator == std::string::npos){
		resultPair.second = resultPair.first;
	}else{
		resultPair.second = std::stod(wfsParameterString.substr(firstEnd+1, rangeEnd-firstEnd -1));
	}
	std::cerr<<"getParameterRangeFromOGCUri openGisUri: "<<wfsParameterString<<" resultPair.first: "<<resultPair.first<<" resultPair.second: "<<resultPair.second<<std::endl;
	return resultPair;
}

int getWfsParameterInteger(const std::string &wfsParameterString){

	size_t rangeStart = wfsParameterString.find_first_of("(");
	size_t rangeEnd = wfsParameterString.find_last_of(")");
	size_t rangeSeperator = wfsParameterString.find_first_of(",", rangeStart);
	size_t firstEnd = (rangeSeperator == std::string::npos) ? rangeEnd : rangeSeperator;

	if(rangeSeperator != std::string::npos)
		std::cerr<<"[getWFSIntegerParameter] "<<wfsParameterString<<" contains a range!"<<std::endl;

	int parameterValue = std::stoi(wfsParameterString.substr(rangeStart+1, firstEnd-rangeStart -1));
	return parameterValue;
}

//std::pair<double, double> getTimeInformationFromOGCUri(std::string wfsParameterString){
//
//
//}


int processWCS(std::map<std::string, std::string> &params) {

	/*http://www.myserver.org:port/path?
	 * service=WCS &version=2.0
	 * &request=GetCoverage
	 * &coverageId=C0002
	 * &subset=lon,http://www.opengis.net/def/crs/EPSG/0/4326(-71,47)
	 * &subset=lat,http://www.opengis.net/def/crs/EPSG/0/4326(-66,51)
	 * &subset=t,http://www.opengis.net/def/trs/ISO-8601/0/Gregorian+UTC("2009-11-06T23:20:52Z")
	 */

	std::string version = params["version"];
	if (version != "2.0.1")
		abort("Invalid version");

	if(params["request"] == "getcoverage") {
		//for now we will handle the OpGraph as the coverageId
		auto graph = GenericOperator::fromJSON(params["coverageid"]);

		//now we will identify the parameters for the QueryRectangle
		std::pair<std::string, std::string> crsInformation = getCrsInformationFromOGCUri(params["outputcrs"]);
		epsg_t query_crsId = std::stoi(crsInformation.second);

		/*
		 *
		 * std::pair<std::string, std::string> crsInformationLon = getCrsInformationFromOGCUri(params["subset_lon"]);
		 * std::pair<std::string, std::string> crsInformationLat = getCrsInformationFromOGCUri(params["subset_lat"]);
		 *
		 * if(crsInformationLat.first != crsInformationLon.first || crsInformationLat.second != crsInformationLon.second){
		 *	std::cerr<<"plz no mixed CRSs! lon:"<<crsInformationLon.second<<"lat: "<<crsInformationLat.second<<std::endl;
		 *	return 1;
		 *}
		 */

		std::pair<double, double> crsRangeLon = getWfsParameterRangeDouble(params["subset_lon"]);
		std::pair<double, double> crsRangeLat = getWfsParameterRangeDouble(params["subset_lat"]);

		unsigned int sizeX = getWfsParameterInteger(params["size_x"]);
		unsigned int sizeY = getWfsParameterInteger(params["size_y"]);

		//TODO: parse datetime!

		//build the queryRectangle and get the data
		QueryRectangle query_rect{42, crsRangeLat.first, crsRangeLon.first, crsRangeLat.second, crsRangeLon.second, sizeX, sizeY, query_crsId};
		QueryProfiler profiler;
		auto result_raster = graph->getCachedRaster(query_rect, profiler);

		//setup the output parameters
		std::string gdalDriver = "GTiff";
		std::string gdalPrefix = "/vsimem/";
		std::string gdalFileName = "test.tif";
		std::string gdalOutFileName = gdalPrefix+gdalFileName;

		//write the raster into a GDAL file
		result_raster.get()->toGDAL(gdalOutFileName.c_str(), gdalDriver.c_str());

		//get the bytearray (buffer) and its size
		vsi_l_offset length;
		GByte* outDataBuffer = VSIGetMemFileBuffer(gdalOutFileName.c_str(), &length, true);

		//put the HTML headers for download
		std::cout<<"Content-Disposition: attachment; filename=\""<<gdalFileName<<"\""<<"\r\n";
		std::cout<<"Content-Length: "<< static_cast<unsigned long long>(length)<<"\r\n";
		std::cout<<"\r\n"; //end of headers

		//write the data into the output stream
		std::cout.write(reinterpret_cast<char*>(outDataBuffer), static_cast<unsigned long long>(length));

		//clean the GDAL resources
		VSIFree(outDataBuffer);

		return 0;
	}

	return 1;
}

epsg_t epsg_from_param(const std::string &crs, epsg_t def = EPSG_WEBMERCATOR) {
	if (crs == "")
		return def;
	if (crs.compare(0,5,"EPSG:") == 0)
		return std::stoi(crs.substr(5, std::string::npos));
	throw ArgumentException("Unknown CRS specified");
}

epsg_t epsg_from_param(const std::map<std::string, std::string> &params, const std::string &key, epsg_t def = EPSG_WEBMERCATOR) {
	if (params.count(key) < 1)
		return def;
	return epsg_from_param(params.at(key), def);
}


int main() {
	//printf("Content-type: text/plain\r\n\r\nDebugging:\n");
	try {
		const char *query_string = getenv("QUERY_STRING");
		if (!query_string) {
			//query_string = "SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image%2Fpng8&TRANSPARENT=true&LAYERS=load&TILED=true&STYLES=dem&CRS=EPSG%3A3857&WIDTH=2571&HEIGHT=1350&BBOX=-8412792.231579678%2C-5618420.447247234%2C16741716.532732407%2C7589898.040431223";
			abort("No query string given");
		}

		std::map<std::string, std::string> params = parseQueryString(query_string);

		epsg_t query_epsg = epsg_from_param(params, "crs", EPSG_WEBMERCATOR);

		time_t timestamp = 42;
		if (params.count("timestamp") > 0) {
			timestamp = std::stol(params["timestamp"]);
		}

		bool debug = true;
		if (params.count("debug") > 0) {
			debug = params["debug"] == "1";
		}

		// direct loading of a query (obsolete?)
		if (params.count("query") > 0) {
			auto graph = GenericOperator::fromJSON(params["query"]);
			int timestamp = 42;
			std::string colorizer;
			if (params.count("colors") > 0)
				colorizer = params["colors"];

			QueryProfiler profiler;
			auto raster = graph->getCachedRaster(QueryRectangle(timestamp, -20037508, 20037508, 20037508, -20037508, 1024, 1024, query_epsg), profiler);

			outputImage(raster.get(), false, false, colorizer);
			return 0;
		}

		// PointCollection as GeoJSON
		if (params.count("pointquery") > 0) {
			auto graph = GenericOperator::fromJSON(params["pointquery"]);

			QueryProfiler profiler;
			auto points = graph->getCachedPoints(QueryRectangle(timestamp, /*-180, -90, 180, 90*/-20037508, 20037508, 20037508, -20037508, 1024, 1024, query_epsg), profiler);

			std::string format("geojson");
			if(params.count("format") > 0)
				format = params["format"];
			if (format == "csv") {
				outputPointCollectionCSV(points.get());
			}
			else if (format == "geojsonfull") {
				outputPointCollection(points.get(), true);
			}
			else {
				outputPointCollection(points.get(), false);
			}
			return 0;
		}

		// Geometry as GeoJSON
		if (params.count("geometryquery") > 0) {
			auto graph = GenericOperator::fromJSON(params["geometryquery"]);

			QueryProfiler profiler;
			auto geometry = graph->getCachedGeometry(QueryRectangle(timestamp, -20037508, 20037508, 20037508, -20037508, 1024, 1024, query_epsg), profiler);

			outputGeometry(geometry.get());
			return 0;
		}

		if(params.count("service") > 0 && params["service"] == "WFS") {
			query_epsg = epsg_from_param(params, "srsname", query_epsg);
			processWFS(params, query_epsg, timestamp);
			return 0;
		}

		// WCS-Requests
		if (params.count("service") > 0 && params["service"] == "WCS") {
			return processWCS(params);
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
				if (version != "1.3.0")
					abort("Invalid version");

				int output_width = std::stoi(params["width"]);
				int output_height = std::stoi(params["height"]);
				if (output_width <= 0 || output_height <= 0) {
					abort("output_width not valid");
				}

				try {
					// Wir ignorieren:
					// format
					// transparent

					// Unbekannt:
					// &STYLES=dem

					//if (params["tiled"] != "true")
					//	abort("only tiled for now");

					double bbox[4];
					parseBBOX(bbox, params.at("bbox"), query_epsg, false);

					auto graph = GenericOperator::fromJSON(params["layers"]);
					std::string colorizer;
					if (params.count("colors") > 0)
						colorizer = params["colors"];

					std::string format("image/png");
					if (params.count("format") > 0) {
						format = params["format"];
					}

					QueryRectangle qrect(timestamp, bbox[0], bbox[1], bbox[2], bbox[3], output_width, output_height, query_epsg);

					if (format == "application/json") {
						QueryProfiler profiler;
						std::unique_ptr<GenericPlot> dataVector = graph->getCachedPlot(qrect, profiler);

						printf("content-type: application/json\r\n\r\n");
						printf(dataVector->toJSON().c_str());
					}
					else {
						QueryProfiler profiler;
						auto result_raster = graph->getCachedRaster(qrect, profiler);

						if (result_raster->lcrs.size[0] != (uint32_t) output_width || result_raster->lcrs.size[1] != (uint32_t) output_height) {
							result_raster = result_raster->scale(output_width, output_height);
						}

						bool flipx = (bbox[2] > bbox[0]) != (result_raster->lcrs.scale[0] > 0);
						bool flipy = (bbox[3] > bbox[1]) == (result_raster->lcrs.scale[1] > 0);

						std::unique_ptr<Raster2D<uint8_t>> overlay;
						if (debug) {
							DataDescription dd_overlay(GDT_Byte, 0, 1);
							overlay.reset( (Raster2D<uint8_t> *) GenericRaster::create(result_raster->lcrs, dd_overlay).release());
							overlay->clear(0);

							// Write debug info
							std::ostringstream msg_tl;
							msg_tl.precision(2);
							msg_tl << std::fixed << bbox[0] << ", " << bbox[1];
							overlay->print(4, 4, 1, msg_tl.str().c_str());

							std::ostringstream msg_br;
							msg_br.precision(2);
							msg_br << std::fixed << bbox[2] << ", " << bbox[3];
							std::string msg_brs = msg_br.str();
							overlay->print(overlay->lcrs.size[1]-4-8*msg_brs.length(), overlay->lcrs.size[1]-12, overlay->dd.max, msg_brs.c_str());

							if (result_raster->lcrs.size[1] >= 512) {
								auto messages = get_debug_messages();
								int ypos = 36;
								for (auto &msg : messages) {
									overlay->print(4, ypos, overlay->dd.max, msg.c_str());
									ypos += 10;
								}
							}
						}

						outputImage(result_raster.get(), flipx, flipy, colorizer, overlay.get());
					}
				}
				catch (const std::exception &e) {
					// Alright, something went wrong.
					// We're still in a WMS request though, so do our best to output an image with a clear error message.

					DataDescription dd(GDT_Byte, 0, 255, true, 0);
					LocalCRS lcrs(EPSG_UNKNOWN, output_width, output_height, 0.0, 0.0, 1.0, 1.0);

					auto errorraster = GenericRaster::create(lcrs, dd);
					errorraster->clear(0);

					auto msg = e.what();
					errorraster->printCentered(1, msg);

					outputImage(errorraster.get(), false, false, "hsv");
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
