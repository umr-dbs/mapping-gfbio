__kernel void matrixkernel(__global const IN_TYPE0 *in_data, __global const RasterInfo *in_info, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, const int matrix_size, __global const int *matrix) {
	const uint posx = get_global_id(0);
	const uint posy = get_global_id(1);
	const int gid = posy * in_info->size[0] + posx;

	if (posx >= out_info->size[0] || posy >= out_info->size[1])
		return;

	int matrix_offset = matrix_size / 2;

	OUT_TYPE0 value = 0;
	for (int ky=0;ky<matrix_size;ky++) {
		for (int kx=0;kx<matrix_size;kx++) {
			int source_x = clamp(posx+kx-matrix_offset, 0u, in_info->size[0]-1);
			int source_y = clamp(posy+ky-matrix_offset, 0u, in_info->size[1]-1);

			IN_TYPE0 v = R(in,source_x,source_y);
			if (in_info->has_no_data && value == in_info->no_data) {
				value = out_info->no_data;
				kx = ky = matrix_size;
				break;
			}
			value += matrix[ky*matrix_size+kx] * v;
		}
	}

	OUT_TYPE0 max = out_info->max;
	OUT_TYPE0 min = out_info->min;

	// Clamp to [min, max] unless it is no_data
	if (!(ISNODATA0(value, in_info))) {
		if (value > max) value = max;
		if (value < min) value = min;
	}

	R(out, posx, posy) = value;
}
