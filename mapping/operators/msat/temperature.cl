__kernel void temperaturekernel(__global const IN_TYPE0 *in_data, __global const RasterInfo *in_info, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, __global float *LuT) {
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
	out_data[gid] = LuT[value];
}
