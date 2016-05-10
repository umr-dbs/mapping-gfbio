
__kernel void difference(const int m_count, const int s_count, __global const double2 *m_coords, __global const double2 *s_coords, __global char *keep, double epsilon) {
	const int gid = get_global_id(0);
	if (gid >= m_count)
		return;

	char keep_this = true;
	double2 coords = m_coords[gid];

	for (int i=0;i<s_count;i++) {
		double2 coords2 = s_coords[i];
		if (length(coords - coords2) <= epsilon) {
			keep_this = false;
			break;
		}
	}

	keep[gid] = keep_this;
}
