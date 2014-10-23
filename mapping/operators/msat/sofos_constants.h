// include guard
#ifndef __CLOUDCLASS_H_INCLUDED__
#define __CLOUDCLASS_H_INCLUDED__

namespace cloudclass{

/* The following constants are used by the SOFOS cloud classification.
 * Re-implementation is based on the available articles (e.g. http://www.sciencedirect.com/science/article/pii/S0098300408000691#),
 * the source code of FMET (http://sourceforge.net/projects/fmet/) and personal communication with Jörg Bendix, Thomas Nauß and Boris Thies.
 * TODO: use enum class with type in AMP++?
 */
static const uint16_t is_no_data			= 0;

static const uint16_t is_surface			= 1;						//0000 0000 0000 0001 - no cloud
static const uint16_t is_snow 				= 1 << 1;						//0000 0000 0000 0010 - snow covered surface -> no cloud
static const uint16_t is_cloud 				= 1 << 2;						//0000 0000 0000 0100 - cloud
static const uint16_t range_surface			= is_surface | is_cloud | is_snow;	//0000 0000 0000 0111 - this is the range of bits indicating surface of cloud

static const uint16_t is_day 				= 1 << 4;						//0000 0000 0000 1000 - day -> no day = night
static const uint16_t is_twilight 			= 1 << 3;						//0000 0000 0001 0000 - twilight TODO: define what is twilight... (IIRC the threshold for SZEN is 60°)
static const uint16_t range_illumination	= is_day | is_twilight;			//0000 0000 0001 1000 - this is the range indicating the illumination condition

static const uint16_t is_cirrus				= 1 << 5;						//0000 0000 0010 0000 - Cirrcus cloud
static const uint16_t is_cirrus_above		= 1 << 6;						//0000 0000 0100 0000 - TODO: define
static const uint16_t is_cumulus 			= 1 << 7;						//0000 0000 1000 0000 - Cumulus cloud
static const uint16_t is_stratus			= 1 << 8;						//0000 0001 0000 0000 - Stratus cloud
static const uint16_t range_cloud_type		= is_cirrus | is_cirrus_above | is_cumulus | is_stratus;	//0000 0001 1110 0000 - the range indicating the cloud type

static const uint16_t is_water				= 1 << 9;						//0000 0010 0000 0000 - cloud phase is water
static const uint16_t is_ice				= 1 << 10;						//0000 0100 0000 0000 - cloud phase is ice
static const uint16_t range_phase			= is_water | is_ice;				//0000 0110 0000 0000 - the range indicating the water phase


static const uint16_t is_low				= 1 << 11;						//0000 1000 0000 0000 - low cloud
static const uint16_t is_low_medium			= 1 << 12;						//0001 0000 0000 0000 - between low and medium high cloud
static const uint16_t is_medium				= 1 << 13;						//0010 0000 0000 0000 - medium high cloud
static const uint16_t is_medium_high		= 1 << 14;						//0100 0000 0000 0000 - between medium high and high cloud
static const uint16_t is_high				= 1 << 15;						//1000 0000 0000 0000 - high cloud
static const uint16_t range_height			= is_low | is_low_medium | is_medium | is_medium_high | is_high; //1111 1000 0000 0000 - the range indicating the estimated cloud height

static const uint16_t range_classification  = range_surface | range_illumination | range_cloud_type | range_phase | range_height;


}

#endif // __CLOUDCLASS_H_INCLUDED__
