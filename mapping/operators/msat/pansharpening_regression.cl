__kernel void pan_regression(__global const IN_TYPE0 *in_data1 , __global const RasterInfo *in_info1, __global const IN_TYPE1 *in_data2 , __global const RasterInfo *in_info2, __global OUT_TYPE0 *out_data1, __global const RasterInfo *out_info1, __global OUT_TYPE0 *out_data2, __global const RasterInfo *out_info2, const int matrix_size, const int distanceWeighting) {
	const int posx = get_global_id(0);
	const int posy = get_global_id(1);
	const int gid = posy * in_info1->size[0] + posx;

	//const int local_id = get_local_id(0);
	//const int group_id = get_group_id(0);
	//const int numItems = get_local_size(0);
	//TODO roundWindow in parameter setzen...
	const int roundWindow =0;

	//consider boundarys...
	if (posx >= out_info1->size[0] || posy >= out_info1->size[1])
		return;

	IN_TYPE0 value1 = in_data1[gid];
	IN_TYPE1 value2 = in_data2[gid];
	if (ISNODATA0(value1, in_info1) || ISNODATA1(value2, in_info2) ) {
		out_data1[gid] = out_info1->no_data;
		out_data2[gid] = out_info2->no_data;
		return;
	}

	//initial parameters for the regression
	int matrix_offset = (matrix_size-1) / 2;
	float sum_LNx_LNy = 0; 	// Sum x*y
	float sum_LNx = 0; 		// Sum x
	float sum_LNy = 0; 		// Sum y
	float sum_LNx_2 = 0; 	// Sum (x^2)
	float n =0.0;
	float weight = 1.0;


	//estimate local regression from given pixel (posx,posy) in Raster:
	//iterate local window surronded by the given pixel and estimate parameters for the regression equation
	for (int local_y= posy - matrix_offset; local_y <= posy +matrix_offset;local_y++) {
		for (int local_x= posx - matrix_offset;local_x <= posx + matrix_offset;local_x++) {


			if(local_x >=0 && local_y >= 0 && local_x < in_info2->size[0] && local_y < in_info2->size[1]){

				//TODO rundes fenster optimieren?
				if ((pow((posx - local_x), 2.0f) + pow((posy - local_y), 2.0f)) <= matrix_size / 2.0f || !roundWindow) {

					int local_id = local_y * in_info2->size[0] + local_x;

					float high_low_value = in_data2[local_id];
					float low_value = in_data1[local_id] + 0.1f;

					//consider distanceWeighting
					if(distanceWeighting){
						float dist =  sqrt( pow((posx - local_x), 2.0f)+ pow((posy - local_y),2.0f)) / matrix_offset;
						float distMid =  0.5f / matrix_offset;
						weight = (10000.0f / fmax(dist,distMid));
					}
					n += weight;

					sum_LNx_LNy += log(high_low_value) * log(low_value) * weight;
					sum_LNx += log(high_low_value) * weight;
					sum_LNy += log(low_value) * weight;
					sum_LNx_2 += pow(log(high_low_value), 2.0f) * weight;
				}
			}

		}
	}

	// solve equation by b
	out_data2[gid] = ((n * sum_LNx_LNy - sum_LNx * sum_LNy) / (n * sum_LNx_2 - pow(sum_LNx, 2.0f)));

	//out_data2[gid] = in_data1[gid] + in_data2[gid];

	// Korrektur:sind die in der Regression beruecksichtigten...
	// Pan-Pixel alle gleich oder fast gleich neigt die potenzielle Regression dazu, die Kurve steil
	// drueber zu legen (was ja nicht falsch ist... im Sinne der kleinsten Quadrate)
	// Steigung wird dann auf 0 gesetzt!
	if (out_data2[gid] > 20 || out_data2[gid] < -20) {
		out_data2[gid] = 0;
	}

	// solve equation by a
	float temp= (sum_LNy - out_data2[gid] * sum_LNx) / n;
	out_data1[gid] =  (pow(M_E_F, temp));


}
