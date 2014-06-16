__kernel void matrixkernel(__global const IN_TYPE0 *in_data, __global const RasterInfo *in_info, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, const int matrix_size, __global const int *matrix) {
	int gid = get_global_id(0);
	if (gid >= out_info->size[0]*out_info->size[1]*out_info->size[2])
		return;

	int matrix_offset = matrix_size / 2;

	uint posx = gid % in_info->size[0];
	uint posy = gid / in_info->size[0];

	OUT_TYPE0 value = 0;
	for (int ky=0;ky<matrix_size;ky++) {
		for (int kx=0;kx<matrix_size;kx++) {
			int source_x = clamp(posx+kx-matrix_offset, 0u, in_info->size[0]-1);
			int source_y = clamp(posy+ky-matrix_offset, 0u, in_info->size[1]-1);

			value += matrix[ky*matrix_size+kx] * R(in,source_x,source_y); //in_data[source_y*in_info->size[0]+source_x];
		}
	}

	OUT_TYPE0 max = out_info->max;
	OUT_TYPE0 min = out_info->min;

	if (value > max) value = max;
	if (value < min) value = min;

	out_data[gid] = value;
}
