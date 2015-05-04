/**
* This kernel implements raster reclassification using a similar approach as ArcGIS: http://resources.arcgis.com/en/help/main/10.1/index.html#//005m0000007m000000
* We are using A<=value<=B where the higher range is inclusive -> classes always include the starting value of the range. 
*/
__kernel void classificationByRangeKernel(
	__global const IN_TYPE0 *in_data,
	__global const RasterInfo *in_info,
	__global OUT_TYPE0 *out_data,
	__global const RasterInfo *out_info,
	__constant float *classificationLowerBorder,
	__constant float *classificationUpperBorder,
	__constant int *classificationClasses,
	const int numberOfClasses,
	const int noDataReClass	
	 ) {
	const int posx = get_global_id(0);
	const int posy = get_global_id(1);
	const int gid = posy * out_info->size[0] + posx;

	if (posx >= out_info->size[0] || posy >= out_info->size[1])
		return;

	IN_TYPE0 value = in_data[gid];
	
	
	//place NoData in a class. This can also be the new NoData class...
	if (ISNODATA0(value, in_info)) {
		out_data[gid] = (OUT_TYPE0)  noDataReClass;
		return;
	}
			
	//start by placeing "value" int he no_data "class". 
	int result_class = out_info->no_data;
	
	//sanity check...
	if(numberOfClasses <= 0){
		out_data[gid] = (OUT_TYPE0) result_class;
		return;
	}
	
	//like ArcGIS we use A <= value <= B. BUT unlike ArcGIS if overlapping occures the HIGHER range is inclusive.
	//TODO: check if this is a good bahavior (i think i remember that this is not clear for the users) 
	for(int i=0; i<numberOfClasses; i++) {		
		if( (value >= classificationLowerBorder[i]) && (value <= classificationUpperBorder[i]) )
			result_class = classificationClasses[i];
	}
	
	out_data[gid] = (OUT_TYPE0) result_class;
}
/**
* This kernel is similar to the raster reclassification and uses using the same comparison approach as ArcGIS: http://resources.arcgis.com/en/help/main/10.1/index.html#//005m0000007m000000
* NOTE: this kernel allows to replace ranges by float values 
*/
__kernel void replacementByRangeKernel(
	__global const IN_TYPE0 *in_data,
	__global const RasterInfo *in_info,
	__global OUT_TYPE0 *out_data,
	__global const RasterInfo *out_info,
	__constant float *classificationLowerBorder,
	__constant float *classificationUpperBorder,
	__constant float *classificationClasses,
	const int numberOfClasses,
	const float noDataReClass	
	 ) {
	const int posx = get_global_id(0);
	const int posy = get_global_id(1);
	const int gid = posy * out_info->size[0] + posx;

	if (posx >= out_info->size[0] || posy >= out_info->size[1])
		return;

	IN_TYPE0 value = in_data[gid];
	
	
	//place NoData in a class. This can also be the new NoData class...
	if (ISNODATA0(value, in_info)) {
		out_data[gid] = (OUT_TYPE0)  noDataReClass;
		return;
	}
			
	//start by placeing "value" int he no_data "class". 
	float result_class = out_info->no_data;
	
	//sanity check...
	if(numberOfClasses<=0){
		out_data[gid] = (OUT_TYPE0) result_class;
		return;
	}
		
	//like ArcGIS we use A <= value <= B. BUT unlike ArcGIS if overlapping occures the HIGHER range is inclusive.
	//TODO: check if this is a good bahavior (i think i remember that this is not clear for the users) 
	for(int i=0; i<numberOfClasses; i++) {		
		if( (value >= classificationLowerBorder[i]) && (value <= classificationUpperBorder[i]) )
			result_class = classificationClasses[i];
	}
	
	
	out_data[gid] = (OUT_TYPE0) result_class;
}
