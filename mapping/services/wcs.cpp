
#include "services/ogcservice.h"
#include "operators/operator.h"
#include "datatypes/raster.h"
#include "util/timeparser.h"

class WCSService : public OGCService {
	public:
		WCSService() = default;
		virtual ~WCSService() = default;
		virtual void run(const Params& params, HTTPResponseStream& result, std::ostream &error);
};
REGISTER_HTTP_SERVICE(WCSService, "WCS");


static std::pair<std::string, std::string> getCrsInformationFromOGCUri(std::string openGisUri,std::ostream &error){
	size_t beforeAutorityId = openGisUri.find("crs")+3;
	size_t behindAutorityId = openGisUri.find_first_of("/",beforeAutorityId+1);
	std::string authorityId = openGisUri.substr(beforeAutorityId+1, behindAutorityId-beforeAutorityId-1);
	error<<"getCrsInformationFromOGCUri openGisUri: "<<openGisUri<<" beforeAutorityId: "<<beforeAutorityId<<" behindAutorityId: "<<behindAutorityId<<" authorityId: "<<authorityId<<std::endl;

	//get the crsID
	size_t beforeCrsCode = openGisUri.find_last_of("/");
	size_t behindCrsCode = openGisUri.find_first_of("(", beforeCrsCode);
	if(behindCrsCode == std::string::npos)
		behindCrsCode = openGisUri.length();

	std::string crsCode = openGisUri.substr(beforeCrsCode+1, behindCrsCode-beforeCrsCode-1);
	error<<"getCrsInformationFromOGCUri openGisUri: "<<openGisUri<<" beforeCrsCode: "<<beforeCrsCode<<" behindCrsCode: "<<behindCrsCode<<" crsCode: "<<crsCode<<std::endl;

	//
	return (std::pair<std::string, std::string>{"EPSG",crsCode});
}

static std::pair<double, double> getWcsParameterRangeDouble(std::string wcsParameterString, std::ostream &error){
	std::pair<double, double> resultPair;

	size_t rangeStart = wcsParameterString.find_first_of("(");
	size_t rangeEnd = wcsParameterString.find_last_of(")");
	size_t rangeSeperator = wcsParameterString.find_first_of(",", rangeStart);
	size_t firstEnd = (rangeSeperator == std::string::npos) ? rangeEnd : rangeSeperator;

	resultPair.first = std::stod(wcsParameterString.substr(rangeStart+1, firstEnd-rangeStart -1));

	if(rangeSeperator == std::string::npos){
		resultPair.second = resultPair.first;
	}else{
		resultPair.second = std::stod(wcsParameterString.substr(firstEnd+1, rangeEnd-firstEnd -1));
	}
	error<<"getParameterRangeFromOGCUri openGisUri: "<<wcsParameterString<<" resultPair.first: "<<resultPair.first<<" resultPair.second: "<<resultPair.second<<std::endl;
	return resultPair;
}

static int getWcsParameterInteger(const std::string &wcsParameterString, std::ostream &error){

	size_t rangeStart = wcsParameterString.find_first_of("(");
	size_t rangeEnd = wcsParameterString.find_last_of(")");
	size_t rangeSeperator = wcsParameterString.find_first_of(",", rangeStart);
	size_t firstEnd = (rangeSeperator == std::string::npos) ? rangeEnd : rangeSeperator;

	if(rangeSeperator != std::string::npos)
		error<<"[getWCSIntegerParameter] "<<wcsParameterString<<" contains a range!"<<std::endl;

	int parameterValue = std::stoi(wcsParameterString.substr(rangeStart+1, firstEnd-rangeStart -1));
	return parameterValue;
}


void WCSService::run(const Params& params, HTTPResponseStream& result, std::ostream &error) {

	/*http://www.myserver.org:port/path?
	 * service=WCS &version=2.0
	 * &request=GetCoverage
	 * &coverageId=C0002
	 * &subset=lon,http://www.opengis.net/def/crs/EPSG/0/4326(-71,47)
	 * &subset=lat,http://www.opengis.net/def/crs/EPSG/0/4326(-66,51)
	 * &subset=t,http://www.opengis.net/def/trs/ISO-8601/0/Gregorian+UTC("2009-11-06T23:20:52Z")
	 */

	std::string version = params.get("version");
	if (version != "2.0.1")
		result.send500("Unsupported WCS version");

	if(params.get("request") == "getcoverage") {
		//for now we will handle the OpGraph as the coverageId
		auto graph = GenericOperator::fromJSON(params.get("coverageid"));

		//now we will identify the parameters for the QueryRectangle
		std::pair<std::string, std::string> crsInformation = getCrsInformationFromOGCUri(params.get("outputcrs"), error);
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

		std::pair<double, double> crsRangeLon = getWcsParameterRangeDouble(params.get("subset_lon"), error);
		std::pair<double, double> crsRangeLat = getWcsParameterRangeDouble(params.get("subset_lat"), error);

		unsigned int sizeX = getWcsParameterInteger(params.get("size_x"), error);
		unsigned int sizeY = getWcsParameterInteger(params.get("size_y"), error);

		double timestamp = 1295266500; // 2011-1-17 12:15
		if(params.hasParam("time"))
			timestamp = TimeParser::create(TimeParser::Format::ISO)->parse(params.get("time"));

		//build the queryRectangle and get the data
		bool flipx, flipy;
		QueryRectangle query_rect(
			SpatialReference(query_crsId, crsRangeLat.first, crsRangeLon.first, crsRangeLat.second, crsRangeLon.second, flipx, flipy),
			TemporalReference(TIMETYPE_UNIX, timestamp, timestamp),
			QueryResolution::pixels(sizeX, sizeY)
		);
		QueryProfiler profiler;
		auto result_raster = graph->getCachedRaster(query_rect,profiler);
		// TODO: Raster flippen?

		//setup the output parameters
		std::string gdalDriver = "GTiff";
		std::string gdalPrefix = "/vsimem/";
		std::string gdalFileName = "test.tif";
		std::string gdalOutFileName = gdalPrefix+gdalFileName;

		//write the raster into a GDAL file
		result_raster->toGDAL(gdalOutFileName.c_str(), gdalDriver.c_str());

		//get the bytearray (buffer) and its size
		vsi_l_offset length;
		GByte* outDataBuffer = VSIGetMemFileBuffer(gdalOutFileName.c_str(), &length, true);

		//put the HTML headers for download
		//result.sendContentType("???"); // TODO
		result.sendHeader("Content-Disposition", concat("attachment; filename=\"",gdalFileName,"\""));
		result.sendHeader("Content-Length", concat(static_cast<size_t>(length)));
		result.finishHeaders();

		//write the data into the output stream
		result.write(reinterpret_cast<char*>(outDataBuffer), static_cast<size_t>(length));

		//clean the GDAL resources
		VSIFree(outDataBuffer);
	}
}
