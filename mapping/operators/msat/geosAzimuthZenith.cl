#pragma OPENCL EXTENSION cl_khr_fp64 : enable
#define dEarthMeanRadius     6371.01	// In km
#define dAstronomicalUnit    149597890	// In km
#define sub_lon 0.0						// sub satellite point!


double2 transformGeosToLatLon(double2 geosPosition){
	double2 xy = radians(geosPosition);
	
	double2 sinxy = sin(xy);
	double2 cosxy = cos(xy);
	
	double cos2y = pown(cosxy.y, 2);
	double sin2y = pown(sinxy.y, 2);
	double cosxcosy = cosxy.x * cosxy.y;
	double cos2yconstsin2y = cos2y + 1.006803 * sin2y;

	double sd_part1 = pown(42164 * cosxcosy, 2);
	double sd_part2 = cos2yconstsin2y * 1737121856;
	double sd = sqrt(sd_part1 - sd_part2);
	double sn_part1 = 42164 * cosxcosy - sd;
	double sn_part2 = cos2yconstsin2y;
	double sn = sn_part1 / sn_part2;
	double s1 = 42164 - sn * cosxcosy;
	double s2 = sn * sinxy.x * cosxy.y;
	double s3 = -1.0 * sn * sinxy.y;
	double sxy = sqrt(pown(s1, 2) + pown(s2, 2));

	double2 latLonRad;
	double lonRad_part = s2 / s1;
	latLonRad.y = atan(lonRad_part) + radians(sub_lon);
	double latRad_part = 1.006804 * s3 / sxy;
	latLonRad.x = atan(latRad_part);
	
	return degrees(latLonRad);
}

double2 solarAzimuthZenith(double dGreenwichMeanSiderealTime, double dRightAscension, double dDeclination, double2 dLatLon){
		
		double2 azimuthZenith;

		double dLocalMeanSiderealTime = radians(dGreenwichMeanSiderealTime*15 + dLatLon.y);;
		double dHourAngle = dLocalMeanSiderealTime - dRightAscension;
		double dLatitudeInRadians = radians(dLatLon.x);
		double dCos_Latitude = cos( dLatitudeInRadians );
		double dSin_Latitude = sin( dLatitudeInRadians );
		double dCos_HourAngle= cos( dHourAngle );
		
		azimuthZenith.y = (acos( dCos_Latitude*dCos_HourAngle*cos(dDeclination) + sin( dDeclination )*dSin_Latitude));
		
		
		double dY = -sin( dHourAngle );
		double dX = tan( dDeclination )*dCos_Latitude - dSin_Latitude*dCos_HourAngle;
		azimuthZenith.x = atan2( dY, dX );
		
		if ( azimuthZenith.x < 0.0 ) 
			azimuthZenith.x = azimuthZenith.x + M_PI*2;
		//udtSunCoordinates->dAzimuth = udtSunCoordinates->dAzimuth/rad;
		// Parallax Correction
		double dParallax=(dEarthMeanRadius/dAstronomicalUnit)*sin(azimuthZenith.y);
		azimuthZenith.y=(azimuthZenith.y + dParallax);///rad;
			
	return degrees(azimuthZenith);
}

__kernel void azimuthKernel(__global const IN_TYPE0 *in_data, __global const RasterInfo *in_info, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, const double dGreenwichMeanSiderealTime, const double dRightAscension, const double dDeclination) {
	int gid = get_global_id(0);
	if (gid >= in_info->size[0]*in_info->size[1]*in_info->size[2])
		return;
	IN_TYPE0 value = in_data[gid];
	if (ISNODATA0(value, in_info)) {
		out_data[gid] = out_info->no_data;
		return;
	}
	
	//RasterInfo should provide GEOS coordinates
	double2 geosPosition;
	geosPosition.x = ((gid % in_info->size[0]) * in_info->scale[0] + in_info->origin[0]);
	geosPosition.y = ((gid / in_info->size[0]) * in_info->scale[1] + in_info->origin[1]); 
	
	double2 latLonPosition = transformGeosToLatLon(geosPosition);
	double2 azimuthZenith = solarAzimuthZenith(dGreenwichMeanSiderealTime, dRightAscension, dDeclination, latLonPosition);
			
	OUT_TYPE0 result = latLonPosition.x;
	out_data[gid] = result;
}

__kernel void zenithKernel(__global const IN_TYPE0 *in_data, __global const RasterInfo *in_info, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, const double dGreenwichMeanSiderealTime, const double dRightAscension, const double dDeclination) {
	int gid = get_global_id(0);
	if (gid >= in_info->size[0]*in_info->size[1]*in_info->size[2])
		return;
	IN_TYPE0 value = in_data[gid];
	if (ISNODATA0(value, in_info)) {
		out_data[gid] = out_info->no_data;
		return;
	}
	
	//RasterInfo should provide GEOS coordinates
	double2 geosPosition;
	geosPosition.x = ((gid % in_info->size[0]) * in_info->scale[0] + in_info->origin[0]);
	geosPosition.y = ((gid / in_info->size[0]) * in_info->scale[1] + in_info->origin[1]); 
	
	double2 latLonPosition = transformGeosToLatLon(geosPosition);
	double2 azimuthZenith = solarAzimuthZenith(dGreenwichMeanSiderealTime, dRightAscension, dDeclination, latLonPosition);
			
	OUT_TYPE0 result = azimuthZenith.y;
	out_data[gid] = result;
}