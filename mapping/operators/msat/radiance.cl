__kernel void radianceKernel(__global const IN_TYPE0 *in_data, __global const RasterInfo *in_info, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, float offset, float slope) {
	const int posx = get_global_id(0);
	const int posy = get_global_id(1);
	const int gid = posy * out_info->size[0] + posx;

	if (posx >= out_info->size[0] || posy >= out_info->size[1])
		return;

	IN_TYPE0 value = in_data[gid];
	if (ISNODATA0(value, in_info)) {
		out_data[gid] = out_info->no_data;
		return;
	}
	OUT_TYPE0 result = offset + value * slope;
	out_data[gid] = result;
}


__kernel void radianceConvertedKernel(__global const IN_TYPE0 *in_data, __global const RasterInfo *in_info, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, const float offset, const float slope, const float conversionFactor) {
	const int posx = get_global_id(0);
	const int posy = get_global_id(1);

	if (posx >= out_info->size[0] || posy >= out_info->size[1])
		return;

	IN_TYPE0 value = R(in, posx, posy);
	if (ISNODATA0(value, in_info)) {
		R(out, posx, posy) = out_info->no_data;
		return;
	}
	OUT_TYPE0 result = (offset + value * slope) * conversionFactor;
	R(out, posx, posy) = result;
}
