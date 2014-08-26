#pragma OPENCL EXTENSION cl_khr_fp64 : enable

__kernel void reflectancekernel(__global const IN_TYPE0 *in_data, __global const RasterInfo *in_info, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, const double dGreenwichMeanSiderealTime, const double dRightAscension, const double dDeclination, const double dETSRconst, const double dESD) {
	int gid = get_global_id(0);
	if (gid >= in_info->size[0]*in_info->size[1]*in_info->size[2])
		return;
	IN_TYPE0 value = in_data[gid];
	if (ISNODATA0(value, in_info)) {
		out_data[gid] = out_info->no_data;
		return;
	}
	OUT_TYPE0 result = value * dESD / dETSRconst;
	out_data[gid] = result;
}
