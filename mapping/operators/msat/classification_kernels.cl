__kernel void multiThresholdKernel(__global const IN_TYPE0 *in_data,  __global const RasterInfo *in_info, __global OUT_TYPE0 *out_data, __global const RasterInfo *out_info, __constant IN_TYPE0 *thresholds, __constant OUT_TYPE0 *thresholdBitmasks, const int numberOfThresholds) {
	int gid = get_global_id(0);
	if (gid >= in_info->size[0]*in_info->size[1]*in_info->size[2])
		return;
	IN_TYPE0 value = in_data[gid];
	if (ISNODATA0(value, in_info)) {
		out_data[gid] = out_info->no_data;
		return;
	}
			
	OUT_TYPE0 originalBitmask = 0;
	OUT_TYPE0 newBitmask = thresholdBitmasks[0];
	IN_TYPE0 lowerBound = thresholds[0];
	
	if(value < lowerBound)
		newBitmask = (thresholdBitmasks[0] | originalBitmask);

	for(int i=1; i<numberOfThresholds; i++) {	
		IN_TYPE0 upperBound = thresholds[i];
		
		if( (value >= lowerBound) && (value <= upperBound) )
			newBitmask =(thresholdBitmasks[i] | originalBitmask);
				
		//printf("OCL gid = %d, val = %f, low = %f, hi = %f , bitOld= %d, bitNew= %d \n",gid,  value, lowerBound, upperBound, originalBitmask, newBitmask);		
		lowerBound = upperBound;		
	}
	
	if(value > lowerBound)
		newBitmask = (thresholdBitmasks[0] | originalBitmask);
	
	//printf("OCL gid = %d, val = %f , bitOld= %d, bitNew= %d \n",gid,  value, originalBitmask, newBitmask);
	
	
	out_data[gid] = newBitmask;
}