
#include "services/ogcservice.h"
#include "operators/operator.h"
#include "datatypes/raster.h"
#include "util/timeparser.h"

/**
 * Implementation of the OGC WCS standard http://www.opengeospatial.org/standards/wcs
 * It currently only supports our specific use cases
 */
class WCSService : public OGCService {
	public:
		using OGCService::OGCService;
		virtual ~WCSService() = default;
		virtual void run();
};
REGISTER_HTTP_SERVICE(WCSService, "WCS");

/**
 * This method extracts the CRS information from a semantic opengis.net uri.
 * It accepts simple CRS strings like http://www.opengis.net/def/crs/EPSG/0/4326
 * It accepts complex strings which include a semantic opengis.net CRS like "lon,http://www.opengis.net/def/crs/EPSG/0/4326(-71,47)"
 */
static std::pair<std::string, std::string> getCrsInformationFromOGCUri(const std::string &openGisUri){
	const size_t beforeAutorityId = openGisUri.find("crs")+3;
	const size_t behindAutorityId = openGisUri.find_first_of("/",beforeAutorityId+1);
	const std::string authorityId = openGisUri.substr(beforeAutorityId+1, behindAutorityId-beforeAutorityId-1);

	//get the crsID
	size_t beforeCrsCode = openGisUri.find_last_of("/");
	size_t behindCrsCode = openGisUri.find_first_of("(", beforeCrsCode);
	if(behindCrsCode == std::string::npos)
		behindCrsCode = openGisUri.length();

	std::string crsCode = openGisUri.substr(beforeCrsCode+1, behindCrsCode-beforeCrsCode-1);

	return (std::pair<std::string, std::string>{"EPSG",crsCode});
}


/**
 * This method extracts a (double) parameter range from a wcs string. The string may start with a full semantic opengis.net uri.
 * Examples: &subset=x,(-71,47), &subset=lon,http://www.opengis.net/def/crs/EPSG/0/4326(-71,47)
 */
static std::pair<double, double> getWcsParameterRangeDouble(const std::string &wcsParameterString){
	std::pair<double, double> resultPair;

	const size_t rangeStart = wcsParameterString.find_first_of("(");
	const size_t rangeEnd = wcsParameterString.find_last_of(")");
	const size_t rangeSeperator = wcsParameterString.find_first_of(",", rangeStart);
	const size_t firstEnd = (rangeSeperator == std::string::npos) ? rangeEnd : rangeSeperator;

	resultPair.first = std::stod(wcsParameterString.substr(rangeStart+1, firstEnd-rangeStart -1));

	if(rangeSeperator == std::string::npos){
		resultPair.second = resultPair.first;
	}else{
		resultPair.second = std::stod(wcsParameterString.substr(firstEnd+1, rangeEnd-firstEnd -1));
	}
	return resultPair;
}


/**
 * This method extracts a single int parameter from a wcs string. This is used for params like &size_x=(3712)
 */
static int getWcsParameterInteger(const std::string &wcsParameterString){

	const size_t rangeStart = wcsParameterString.find_first_of("(");
	const size_t rangeEnd = wcsParameterString.find_last_of(")");
	const size_t rangeSeperator = wcsParameterString.find_first_of(",", rangeStart);

	//TODO: currently we only support single time stamps. WCS does allow to query for a time range which should be handled somehow.
	const size_t firstEnd = (rangeSeperator == std::string::npos) ? rangeEnd : rangeSeperator;

	int parameterValue = std::stoi(wcsParameterString.substr(rangeStart+1, firstEnd-rangeStart -1));
	return parameterValue;
}


void WCSService::run() {

	/*http://www.myserver.org:port/path?
	 * service=WCS &version=2.0
	 * &request=GetCoverage
	 * &coverageId=C0002
	 * &subset=lon,(-71,47)
	 * &subset=lat,(-66,51)
	 * &subset=t,http://www.opengis.net/def/trs/ISO-8601/0/Gregorian+UTC("2009-11-06T23:20:52Z")
	 * &OUTPUTCRS=http://www.opengis.net/def/crs/EPSG/0/4326
	 * &SCALESIZE=axis(pixel)[,axis(size)]
	 */

	std::string version = params.get("version");
	if (version != "2.0.1")
		response.send500("Unsupported WCS version");

	if(params.get("request") == "getcoverage") {
		//for now we will handle the OpGraph as the coverageId
		auto graph = GenericOperator::fromJSON(params.get("coverageid"));

		//now we will identify the parameters for the QueryRectangle
		std::pair<std::string, std::string> crsInformation = getCrsInformationFromOGCUri(params.get("outputcrs"));
		epsg_t query_crsId = (epsg_t) std::stoi(crsInformation.second);

		/*
		 *
		 * std::pair<std::string, std::string> crsInformationLon = getCrsInformationFromOGCUri(params["subset_lon"]);
		 * std::pair<std::string, std::string> crsInformationLat = getCrsInformationFromOGCUri(params["subset_lat"]);
		 *
		 * if(crsInformationLat.first != crsInformationLon.first || crsInformationLat.second != crsInformationLon.second){
		 *	error<<"plz no mixed CRSs! lon:"<<crsInformationLon.second<<"lat: "<<crsInformationLat.second<<std::endl;
		 *	return 1;
		 *}
		 */

		//TODO: Handle aliases of coordinate axes like "lon,lat".
		std::pair<double, double> crsRangeX = getWcsParameterRangeDouble(params.get("subset_x"));
		std::pair<double, double> crsRangeY = getWcsParameterRangeDouble(params.get("subset_y"));

		unsigned int sizeX = getWcsParameterInteger(params.get("size_x"));
		unsigned int sizeY = getWcsParameterInteger(params.get("size_y"));

		double timestamp = 1295266500; // 2011-1-17 12:15
		if(params.hasParam("time"))
			timestamp = TimeParser::create(TimeParser::Format::ISO)->parse(params.get("time"));

		//build the queryRectangle and get the data
		bool flipx, flipy;
		QueryRectangle query_rect(
			SpatialReference(query_crsId, crsRangeX.first, crsRangeY.first, crsRangeX.second, crsRangeY.second, flipx, flipy),
			TemporalReference(TIMETYPE_UNIX, timestamp),
			QueryResolution::pixels(sizeX, sizeY)
		);
		QueryProfiler profiler;
		auto result_raster = graph->getCachedRaster(query_rect,profiler);

		auto format = params.get("format", "image/tiff");
		fprintf(stderr,format.c_str());
		bool exportMode = false;
		if(format.find(EXPORT_MIME_PREFIX) == 0) {
			exportMode = true;
			format = format.substr(strlen(EXPORT_MIME_PREFIX));
		}

		GByte* outDataBuffer;
		vsi_l_offset length;
		std::string gdalFileName;
		if(format == "image/tiff") {
			//setup the output parameters
			std::string gdalDriver = "GTiff";
			std::string gdalPrefix = "/vsimem/";
			gdalFileName = "test.tif";
			std::string gdalOutFileName = gdalPrefix+gdalFileName;

			//write the raster into a GDAL file
			result_raster->toGDAL(gdalOutFileName.c_str(), gdalDriver.c_str());

			//get the bytearray (buffer) and its size

			outDataBuffer = VSIGetMemFileBuffer(gdalOutFileName.c_str(), &length, true);
		} else
			throw ArgumentException("WCSService: unknown format");

		if(exportMode) {
			exportZip(reinterpret_cast<char*>(outDataBuffer), static_cast<size_t>(length), format, *graph->getFullProvenance());
		} else {
			//put the HTML headers for download
			//response.sendContentType("???"); // TODO
			response.sendHeader("Content-Disposition", concat("attachment; filename=\"",gdalFileName,"\""));
			response.sendHeader("Content-Length", concat(static_cast<size_t>(length)));
			response.finishHeaders();

			//write the data into the output stream
			response.write(reinterpret_cast<char*>(outDataBuffer), static_cast<size_t>(length));
		}

		//clean the GDAL resources
		VSIFree(outDataBuffer);
	}
}
