// include guard
#ifndef __CLOUDCLASS_H_INCLUDED__
#define __CLOUDCLASS_H_INCLUDED__

namespace cloudclass{

/* The following constants are used by the SOFOS cloud classification.
 * Re-implementation is based on the available articles (e.g. http://www.sciencedirect.com/science/article/pii/S0098300408000691#),
 * the source code of FMET (http://sourceforge.net/projects/fmet/) and personal communication with Jörg Bendix, Thomas Nauß and Boris Thies.
 * TODO: use enum class with type in AMP++?
 */
static const uint16_t rangeSurface			= 3;	//0000 0000 0000 0011 - this is the range of bits indicating surface of cloud
static const uint16_t isSurface				= 0;	//0000 0000 0000 0000 - no cloud
static const uint16_t isCloud 				= 1;	//0000 0000 0000 0001 - cloud
static const uint16_t isSnow 				= 2;	//0000 0000 0000 0010 - snow covered surface -> no cloud

static const uint16_t rangeIllumination		= 12;	//0000 0000 0000 1100 - this is the range indicating the illumination condition
static const uint16_t isDay 				= 4;	//0000 0000 0000 0100 - day -> no day = night
static const uint16_t isTwilight 			= 8;	//0000 0000 0000 1000 - twilight TODO: define what is twilight... (IIRC the threshold for SZEN is 60°)

static const uint16_t rangeCloudType		= 240;	//0000 0000 1111 0000 - the range indicating the cloud type
static const uint16_t isCirrus				= 16;	//0000 0000 0001 0000 - Cirrcus cloud
static const uint16_t isCirrusAbove			= 32;	//0000 0000 0010 0000 - TODO: define
static const uint16_t isCumulus 			= 64;	//0000 0000 0100 0000 - Cumulus cloud
static const uint16_t isStratus				= 128;	//0000 0000 1000 0000 - Stratus cloud

static const uint16_t rangePhase			= 768;	//0000 0011 0000 0000 - the range indicating the water phase
static const uint16_t isWater				= 256;	//0000 0001 0000 0000 - cloud phase is water
static const uint16_t isIce					= 512;	//0000 0010 0000 0000 - cloud phase is ice

static const uint16_t rangeHeight			= 31744;//0111 1100 0000 0000 - the range indicating the estimated cloud height
static const uint16_t isLow					= 1024;	//0000 0100 0000 0000 - low cloud
static const uint16_t isLowMed				= 2048;	//0000 1000 0000 0000 - between low and medium high cloud
static const uint16_t isMed					= 4096;	//0001 0000 0000 0000 - medium high cloud
static const uint16_t isMedHigh				= 8192;	//0010 0000 0000 0000 - between medium high and high cloud
static const uint16_t isHigh				= 16384;//0100 0000 0000 0000 - high cloud

}

#endif // __CLOUDCLASS_H_INCLUDED__
