__kernel void pan_interpolate(__global const IN_TYPE0 *in0_data, __global const RasterInfo *in0_info,__global const IN_TYPE1 *in1_data, __global const RasterInfo *in1_info,__global const IN_TYPE2 *in2_data, __global const RasterInfo *in2_info,__global const IN_TYPE0 *in_data4, __global const RasterInfo *in_info4, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, const int ratio) {
	const size_t posx = get_global_id(0);
	const size_t posy = get_global_id(1);

	if (posx >= out_info->size[0] || posy >= out_info->size[1])
		return;

	size_t posx_low = posx/ratio;
	size_t posy_low = posy/ratio;
	IN_TYPE0 value0 = R(in0, posx_low, posy_low);
	IN_TYPE1 value1 = R(in1, posx_low, posy_low);
	IN_TYPE2 value2 = R(in2, posx, posy);

	if (ISNODATA0(value0, in0_info) || ISNODATA1(value1, in1_info) || ISNODATA2(value2, in2_info)) {
		R(out, posx, posy) = out_info->no_data;
		return;
	}

	//TODO Grenzen high.length/interpol.length beachten
	//use estimated regression parameter a and b fromt the regression equation and solve by x  (value of the hrv raster)
	float result = value0 * pow(value2, value1) - 0.1f;

	//TODO Ausgabe auf Grenzwerte pruefen
	//TODO maxLo und minLo aus daten auslesen un hier abfragen
	//if ( result > 5.2047887 ) {
	//	result = in_data4[posy/ratio * in_info4->size[0] + posx/ratio];
	//}
	result = min(result, out_info->max);

	R(out, posx, posy) = result;
}
