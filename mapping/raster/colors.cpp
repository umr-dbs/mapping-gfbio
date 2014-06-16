

#include "raster/colors.h"
#include "raster/exceptions.h"

#include <cmath>

uint32_t color_from_rgba(uint8_t r, uint8_t g, uint8_t b, uint8_t a) {
	return (uint32_t) a << 24 | (uint32_t) b << 16 | (uint32_t) g << 8 | (uint32_t) r;
}

// h: 0..359, s: 0..255, v: 0..255

// ungenau, evtl. http://www.cs.rit.edu/~ncs/color/t_convert.html nehmen
uint32_t color_from_hsva(uint16_t h, uint8_t s, uint8_t v, uint8_t a) {
    if (s == 0) {
    	return color_from_rgba(v, v, v, a);
    }

    uint8_t p, q, t;
#if 1
	float _h = h / 60.0;
	int region = floor(_h);
	float remainder = _h - region;

	float _v = v / 255.0, _s = s / 255.0;
	p = 255 * _v * ( 1 - _s );
	q = 255 * _v * ( 1 - _s * remainder );
	t = 255 * _v * ( 1 - _s * ( 1.0 - remainder ) );
#else
    uint8_t region, remainder;

    region = h / 60;
    remainder = (h - ((uint16_t) region * 60)) * 4;

    p = (v * (255 - s)) >> 8;
    q = (v * (255 - ((s * remainder) >> 8))) >> 8;
    t = (v * (255 - ((s * (255 - remainder)) >> 8))) >> 8;
#endif

    switch (region) {
        case 0:
        	return color_from_rgba(v, t, p, a);
        case 1:
        	return color_from_rgba(q, v, p, a);
        case 2:
        	return color_from_rgba(p, v, t, a);
        case 3:
        	return color_from_rgba(p, q, v, a);
        case 4:
        	return color_from_rgba(t, p, v, a);
        default:
        	return color_from_rgba(v, p, q, a);
    }
}

Colorizer::Colorizer() : range(0) {
}

Colorizer::~Colorizer() {
}




GreyscaleColorizer::GreyscaleColorizer() : Colorizer() {
}

GreyscaleColorizer::~GreyscaleColorizer() {
}

uint32_t GreyscaleColorizer::colorize(uint32_t data) {
	uint8_t color = (uint8_t) (255.0*data/range);
	return color_from_rgba(color, color, color, 255);
}


HSVColorizer::HSVColorizer() : Colorizer() {
}

HSVColorizer::~HSVColorizer() {
}

uint32_t HSVColorizer::colorize(uint32_t data) {
	return color_from_hsva((uint16_t) (120.0-(120.0*data/range)), 150, 255, 255);
}
