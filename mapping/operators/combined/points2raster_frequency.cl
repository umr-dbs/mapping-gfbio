
__kernel void blur_frequency(__global const IN_TYPE0 *in_data, __global const RasterInfo *in_info, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, const double radius) {
	const int posx = get_global_id(0);
	const int posy = get_global_id(1);

	if (posx >= out_info->size[0] || posy >= out_info->size[1])
		return;

	// The rasters have different size. Transformation should be easy: scale should be identical, and the offset should be exactly radius.
	// Well, should be. If it isn't, our output would contain errors. To avoid rounding errors, we're doing the full transformation through both CRS anyway.
	// Usually, the transformation should look like this:
	/*
	float worldx = (posx+dx) * out_info->scale[0] + out_info->origin[0];
	float worldy = (posy+dy) * out_info->scale[1] + out_info->origin[1];
	int srcx = (worldx - in_info->origin[0]) / in_info->scale[0];
	int srcy = (worldy - in_info->origin[1]) / in_info->scale[1];
	*/
	// We can speed up calculation by reordering those formulas and precalculating parts of it
	// This optimization speeds up the kernel about 9%
	float2 factor, add;
	factor.x = out_info->scale[0] / in_info->scale[0];
	factor.y = out_info->scale[1] / in_info->scale[1];
	add.x = (out_info->origin[0] - in_info->origin[0]) / in_info->scale[0];
	add.y = (out_info->origin[1] - in_info->origin[1]) / in_info->scale[1];


	float value = 0;
	float mindist = 9999999.0;
	for (int dy = -radius;dy<=radius;dy++) {
		for (int dx = -radius;dx<=radius;dx++) {
			int2 src = convert_int2(floor((float2) (posx+dx, posy+dy) * factor + add));

			if (src.x < 0 || src.y < 0 || src.x >= in_info->size[0] || src.y >= in_info->size[1])
				continue;
			IN_TYPE0 count = R(in, src.x, src.y);
			if (count <= 0)
				continue;
			float dist = sqrt((float) (dx*dx + dy*dy));
			if (dist > radius)
				continue;
			mindist = min(dist, mindist);

			value += (1-dist/radius) * 10 * count;
		}
	}
	// Apply a strict maximum colorization based on the distance to the nearest point
	float maxvalue = (1-mindist/radius) * out_info->max;
	value = max(0.0f, min(maxvalue, value));

	R(out, posx, posy) = min(out_info->max, value);
}

