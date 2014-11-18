__kernel void co2correctionkernel(__global const IN_TYPE0 *in_data_bt039, __global const RasterInfo *in_info_bt039, __global const IN_TYPE1 *in_data_bt108, __global const RasterInfo *in_info_bt108, __global const IN_TYPE2 *in_data_bt134, __global const RasterInfo *in_info_bt134, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info) {
	int gid = get_global_id(0);
	if (gid >= out_info->size[0]*out_info->size[1]*out_info->size[2])
		return;
	IN_TYPE0 value_bt039 = in_data_bt039[gid];
	if (ISNODATA0(value_bt039, in_info_bt039)) {
		out_data[gid] = out_info->no_data;
		return;
	}
	IN_TYPE1 value_bt108 = in_data_bt108[gid];
	if (ISNODATA1(value_bt108, in_info_bt108)) {
		out_data[gid] = out_info->no_data;
		return;
	}
	IN_TYPE0 value_bt134 = in_data_bt134[gid];
	if (ISNODATA2(value_bt134, in_info_bt134)) {
		out_data[gid] = out_info->no_data;
		return;
	}
	
	float DTCo2 = (value_bt108 - value_bt134) / 4.0f;
    float RCorr = pow(value_bt108, 4.0f) - (pow(value_bt108 - DTCo2, 4.0f));
    float BT039 = pow(pow(value_bt039, 4.0f) + RCorr, 0.25f);	
	
	out_data[gid] = BT039;
}
