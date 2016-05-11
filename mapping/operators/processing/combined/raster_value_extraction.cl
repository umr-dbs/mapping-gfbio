
__kernel void add_attribute(__global const IN_TYPE0 *in_data, __global const RasterInfo *in_info, const int point_count, __global double2 *point_coords, __global double *point_values) {
	const int gid = get_global_id(0);
	if (gid >= point_count)
		return;

	double2 coords = point_coords[gid];

	int pix_x = round((coords.x - in_info->origin[0]) / in_info->scale[0]);
	int pix_y = round((coords.y - in_info->origin[1]) / in_info->scale[1]);

	double value = NAN;

	if (pix_x >= 0 && pix_y >= 0 && pix_x < in_info->size[0] && pix_y < in_info->size[1]) {
		// the coordinate is inside our raster
		IN_TYPE0 raster_value = R(in, pix_x, pix_y);
		if (!(ISNODATA0(raster_value, in_info))) {
			value = raster_value;
		}
	}

	point_values[gid] = value;
}
