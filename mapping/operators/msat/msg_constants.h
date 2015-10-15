// include guard
#ifndef __MSG_CONSTANTS_H_INCLUDED__
#define __MSG_CONSTANTS_H_INCLUDED__

namespace msg{

	//static const double dETSR[4] = { 519.0318f, 353.8837f, 75.72047f, 445.3367f };
	//static const double dETSRconst[4] = { 20.76f, 23.24f, 19.85f, 25.11f };
	static const double dCwl[12] = { 0.639f, 0.809f, 1.635f, 3.965f, 6.337f, 7.362f, 8.718f, 9.668f, 10.763f, 11.938f, 13.355f, 0.674f };

	// solar irradiance *PI						 VIS006,  VIS008,  IR_016,  IR_039,  WV_062,  WV_073,  IR_087,  IR_097,  IR_108,  IR_120,  IR_134,     HRV
	static const double meteosat_08_ETSR[12] = {65.2296, 73.0127, 62.3715, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 78.7599};
	static const double meteosat_09_ETSR[12] = {65.2065, 73.1869, 61.9923, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 79.0113};
	static const double meteosat_10_ETSR[12] = {65.5148, 73.1807, 62.0208, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 78.9416};
	static const double meteosat_11_ETSR[12] = {65.2656, 73.1692, 61.9416, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 79.0035};

	// VC constant to approximate BBT			 VIS006,  VIS008,  IR_016,   IR_039,   WV_062,   WV_073,   IR_087,  IR_097,   IR_108,  IR_120,  IR_134,    HRV
	static const double meteosat_08_VC[12] =   {0.00000, 0.00000, 0.00000, 2567.330, 1598.103, 1362.081, 1149.069, 1034.343, 930.647, 839.660, 752.387, 0.00000};
	static const double meteosat_09_VC[12] =   {0.00000, 0.00000, 0.00000, 2568.832, 1600.548, 1360.330, 1148.620, 1035.289, 931.700, 836.445, 751.792, 0.00000};
	static const double meteosat_10_VC[12] =   {0.00000, 0.00000, 0.00000, 2547.771, 1595.621, 1360.337, 1148.130, 1034.715, 929.842, 838.659, 750.653, 0.00000};
	static const double meteosat_11_VC[12] =   {0.00000, 0.00000, 0.00000, 2555.280, 1596.080, 1361.748, 1147.433, 1034.851, 931.122, 839.113, 748.585, 0.00000};

	// ALPHA constant to approximate BBT		 VIS006,  VIS008,  IR_016,  IR_039  WV_062, WV_073, IR_087, IR_097, IR_108, IR_120, IR_134,  HRV
	static const double meteosat_08_ALPHA[12]= {0.00000, 0.00000, 0.00000, 0.9956, 0.9962, 0.9991, 0.9996, 0.9999, 0.9983, 0.9988, 0.9981, 0};
	static const double meteosat_09_ALPHA[12]= {0.00000, 0.00000, 0.00000, 0.9954, 0.9963, 0.9991, 0.9996, 0.9999, 0.9983, 0.9988, 0.9981, 0};
	static const double meteosat_10_ALPHA[12]= {0.00000, 0.00000, 0.00000, 0.9915, 0.9960, 0.9991, 0.9996, 0.9999, 0.9983, 0.9988, 0.9982, 0};
	static const double meteosat_11_ALPHA[12]= {0.00000, 0.00000, 0.00000, 0.9916, 0.9959, 0.9990, 0.9996, 0.9998, 0.9983, 0.9988, 0.9981, 0};

	// ALPHA constant to approximate BBT		 VIS006,  VIS008,  IR_016, IR_039, WV_062, WV_073, IR_087, IR_097, IR_108, IR_120, IR_134, HRV
	static const double meteosat_08_BETA[12] = {0.00000, 0.00000, 0.00000, 3.4100, 2.2180, 0.4780, 0.1790, 0.0600, 0.6250, 0.3970, 0.5780, 0};
	static const double meteosat_09_BETA[12] = {0.00000, 0.00000, 0.00000, 3.4380, 2.1850, 0.4700, 0.1790, 0.0560, 0.6400, 0.4080, 0.5610, 0};
	static const double meteosat_10_BETA[12] = {0.00000, 0.00000, 0.00000, 2.9002, 2.0337, 0.4340, 0.1714, 0.0527, 0.6084, 0.3882, 0.5390, 0};
	static const double meteosat_11_BETA[12] = {0.00000, 0.00000, 0.00000, 2.9438, 2.0780, 0.4929, 0.1731, 0.0597, 0.6256, 0.4002, 0.5635, 0};

	static const double c1 = 1.19104273e-16;
	static const double c2 = 0.0143877523;

	// struct representing  the parameters of a Meteosat Satellite
	struct Satellite {
		short meteosat_id;
		short msg_id;
		const double *cwl;
		const double *etsr;
		const double *vc;
		const double *alpha;
		const double *beta;
	};

	// static Satellites
	static const Satellite meteosat_08 {8,  1, dCwl, meteosat_08_ETSR, meteosat_08_VC, meteosat_08_ALPHA, meteosat_08_BETA};
	static const Satellite meteosat_09 {9,  2, dCwl, meteosat_09_ETSR, meteosat_09_VC, meteosat_09_ALPHA, meteosat_09_BETA};
	static const Satellite meteosat_10 {10, 3, dCwl, meteosat_10_ETSR, meteosat_10_VC, meteosat_10_ALPHA, meteosat_10_BETA};
	static const Satellite meteosat_11 {11, 4, dCwl, meteosat_11_ETSR, meteosat_11_VC, meteosat_11_ALPHA, meteosat_11_BETA};



}

#endif // __MSG_CONSTANTS_H_INCLUDED__
