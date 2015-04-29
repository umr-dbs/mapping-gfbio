__kernel void pan_interpolate(__global const IN_TYPE0 *in_data1, __global const RasterInfo *in_info1,__global const IN_TYPE0 *in_data2, __global const RasterInfo *in_info2,__global const IN_TYPE0 *in_data3, __global const RasterInfo *in_info3,__global const IN_TYPE0 *in_data4, __global const RasterInfo *in_info4, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, const int ratio) {
	const uint posx = get_global_id(0);
	const uint posy = get_global_id(1);
	const int gid = posy * out_info->size[0] + posx;


	if (gid>= out_info->size[0]*out_info->size[1]*out_info->size[2])
		return;
	IN_TYPE0 value1 = in_data1[posy/ratio * in_info1->size[0] + posx/ratio];	
	IN_TYPE1 value2 = in_data2[posy/ratio * in_info2->size[0] + posx/ratio];
	IN_TYPE1 value3 = in_data3[gid];
	//IN_TYPE1 value3 = in_data4[posy/ratio * in_info4->size[0] + posx/ratio];
	if (ISNODATA0(value1, in_info1) || ISNODATA1(value2, in_info2) || ISNODATA2(value3, in_info3)) {
		out_data[gid] = out_info->no_data;
		return;
	}

	//TODO Grenzen high.length/interpol.length beachten
	//use estimated regression parameter a and b fromt the regression equation and solve by x  (value of the hrv raster)
	float result = value1 * pow(in_data3[posy * in_info3->size[0] + posx], value2) - 0.1f;

	//TODO Ausgabe auf Grenzwerte pruefen 
	//TODO maxLo und minLo aus daten auslesen un hier abfragen
	//if ( result > 5.2047887 ) {
	//	result = in_data4[posy/ratio * in_info4->size[0] + posx/ratio];
	//}

	out_data[gid] = result;	
}
