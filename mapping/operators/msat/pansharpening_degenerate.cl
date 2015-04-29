__kernel void pan_downsample_spatial(__global const IN_TYPE0 *in_data, __global const RasterInfo *in_info, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, const int sample_ratio, const int matrix_length, __global const float *spatialResponseMatrix) {

	//estimate logic raster coordinates
	const int out_x = get_global_id(0);
	const int out_y = get_global_id(1);
	const int out_gid = out_y * out_info->size[0] + out_x;

	if (out_x >= out_info->size[0] || out_y >= out_info->size[1])
		return;

	//get the input raster coords
	const int in_x = out_x*sample_ratio + sample_ratio / 2;
	const int in_y = out_y*sample_ratio + sample_ratio / 2;
	const int rows = in_info->size[1];
	const int cols = in_info->size[0];



	float val = 0;

	// Gebiet der SpatialResponseMatrix durchlaufen
	for (int local_x = in_x - matrix_length / 2; local_x <= in_x+ matrix_length / 2; local_x++) {
		for (int local_y = in_y - matrix_length / 2; local_y <= in_y + matrix_length / 2; local_y++) {

			if (local_x > 0 && local_y > 0 && local_x < cols && local_y < rows) {

				// Position des betrachteten Bildpixel in der
				// Spatial Response Matrix
				int matrixPixelX = local_x - (in_x - matrix_length / 2);
				int matrixPixelY = local_y - (in_y - matrix_length / 2);

				float addToVal = in_data[local_x + local_y * in_info->size[0] ] * spatialResponseMatrix[ matrixPixelY * matrix_length + matrixPixelX ];

				// Checken, ob in X-Richtung gegenueberliegendes
				// Feld der Matrix evtl. ausserhalb des Bildes liegt
				// => Dann in untersuchter Dimension doppelt
				// gewichten
				if (in_x - (matrixPixelX - matrix_length / 2) < 0 || in_x - (matrixPixelX - matrix_length/ 2) >= cols){
					addToVal *= 2;
				}
				// Checken, ob in Y-Richtung gegenueberliegendes
				// Feld der Matrix evtl. ausserhalb des Bildes liegt
				// => Dann in untersuchter Dimension doppelt
				// gewichten
				if (in_y - (matrixPixelY - matrix_length / 2) < 0 || in_y - (matrixPixelY - matrix_length / 2) >= rows){
					addToVal *= 2;
				}
				val += addToVal;
			}

		}
	}

	out_data[out_gid] = val;

}

//lets try using a generic downsample approach
__kernel void pan_downsample(__global const IN_TYPE0 *in_data, __global const RasterInfo *in_info, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, const int sample_ratio) {
	//estimate logic raster coordinates
	const int out_x = get_global_id(0);
	const int out_y = get_global_id(1);
	const int out_gid = out_y * out_info->size[0] + out_x;

	if (out_x >= out_info->size[0] || out_y >= out_info->size[1])
		return;

	//get the input raster coords
	const int in_x = out_x*sample_ratio;
	const int in_y = out_y*sample_ratio;

	float sample_sum = 0.0;

	//iterate through the ratio window and downsample the high resulotion raster to the low resulotion raster
	for(int sample_y = 0; sample_y < sample_ratio; sample_y++){
		for(int sample_x = 0; sample_x < sample_ratio; sample_x++){
			int in_gid = (in_y + sample_y) * in_info->size[0] + (in_x+sample_x);
			float in_value = in_data[in_gid];
			sample_sum += in_value;
		}
	}
	float sample_mean = sample_sum / (sample_ratio*sample_ratio);
	out_data[out_gid] = sample_mean;
}
