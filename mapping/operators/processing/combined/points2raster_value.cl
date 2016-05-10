
__kernel void blur_value(__global const IN_TYPE0 *in_count_data, __global const RasterInfo *in_count_info, __global const IN_TYPE1 *in_value_data, __global const RasterInfo *in_value_info, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, const double radius) {
	const uint posx = get_global_id(0);
	const uint posy = get_global_id(1);

	if (posx >= out_info->size[0] || posy >= out_info->size[1])
		return;

	// see points2raster_frequency.cl for an explanation of the formulas
	float2 factor, add;
	factor.x = out_info->scale[0] / in_count_info->scale[0];
	factor.y = out_info->scale[1] / in_count_info->scale[1];
	add.x = (out_info->origin[0] - in_count_info->origin[0]) / in_count_info->scale[0];
	add.y = (out_info->origin[1] - in_count_info->origin[1]) / in_count_info->scale[1];

	float weight = 0;
	float value = 0;
	for (int dy = -radius;dy<=radius;dy++) {
		for (int dx = -radius;dx<=radius;dx++) {
			int2 src = convert_int2(floor((float2) (posx+dx, posy+dy) * factor + add));

			if (src.x < 0 || src.y < 0 || src.x >= in_count_info->size[0] || src.y >= in_count_info->size[1])
				continue;
			IN_TYPE0 pcount = R(in_count, src.x, src.y);
			if (pcount <= 0)
				continue;
			float dist = sqrt((float) (dx*dx + dy*dy));
			if (dist > radius)
				continue;

			float localweight = (1-dist/radius);

			weight += localweight;
			value += R(in_value, src.x, src.y) / pcount * localweight;
		}
	}

	if (weight > 0)
		value = max(out_info->min, min(out_info->max, value / weight));
	else
		value = out_info->no_data;

	R(out, posx, posy) = value;
}
