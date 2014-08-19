

#include "raster/colors.h"
#include "raster/exceptions.h"
#include "util/make_unique.h"

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

Colorizer::Colorizer(bool is_absolute) : is_absolute(is_absolute) {
}

Colorizer::~Colorizer() {
}

std::unique_ptr<Colorizer> Colorizer::make(const std::string &name) {
	if (name == "hsv")
		return std::make_unique<HSVColorizer>();
	if (name == "temperature")
		return std::make_unique<TemperatureColorizer>();
	if (name == "height")
		return std::make_unique<HeightColorizer>();
	return std::make_unique<GreyscaleColorizer>();
}




GreyscaleColorizer::GreyscaleColorizer() : Colorizer(false) {
}

GreyscaleColorizer::~GreyscaleColorizer() {
}

void GreyscaleColorizer::fillPalette(uint32_t *colors, int num_colors, double, double) const {
	for (int c = 0; c < num_colors; c++) {
		uint8_t color = (uint8_t) (255.0*c/num_colors);
		colors[c] = color_from_rgba(color, color, color, 255);
	}
}



HSVColorizer::HSVColorizer() : Colorizer(false) {
}

HSVColorizer::~HSVColorizer() {
}

void HSVColorizer::fillPalette(uint32_t *colors, int num_colors, double, double) const {
	for (int c = 0; c < num_colors; c++) {
		double frac = (double) c / num_colors;
		colors[c] = color_from_hsva((uint16_t) (120.0-(120.0*frac)), 150, 255);
	}
}



TemperatureColorizer::TemperatureColorizer() : Colorizer(true) {
}

TemperatureColorizer::~TemperatureColorizer() {
}

void TemperatureColorizer::fillPalette(uint32_t *colors, int num_colors, double min, double max) const {
	double step = (max - min) / num_colors;
	for (int c = 0; c < num_colors; c++) {
		double value = min + c*step;

		value = std::max(-120.0, std::min(180.0, value/4)); // -30 to +45° C
		colors[c] = color_from_hsva(180.0-value, 150, 255);
	}
}


HeightColorizer::HeightColorizer() : Colorizer(true) {
}

HeightColorizer::~HeightColorizer() {
}

void HeightColorizer::fillPalette(uint32_t *colors, int num_colors, double min, double max) const {
	double step = (max - min) / (num_colors-1);
	for (int c = 0; c < num_colors; c++) {
		double value = min + c*step;
		uint32_t color;
		if (value <= 0) { // #AAFFAA
			color = color_from_rgba(170, 255, 170);
		}
		else if (value <= 1000) { // #00FF00
			double scale = 170-(170*value/1000);
			color = color_from_rgba(scale, 255, scale);
		}
		else if (value <= 1200) { // #FFFF00
			double scale = 255*((value-1000)/200);
			color = color_from_rgba(scale, 255, 0);
		}
		else if (value <= 1400) { // #FF7F00
			double scale = 255-128*((value-1200)/200);
			color = color_from_rgba(255, scale, 0);
		}
		else if (value <= 1600) { // #BF7F3F
			double scale = 64*((value-1400)/200);
			color = color_from_rgba(255-scale, 127, scale);
		}
		else if (value <= 2000) { // 000000
			double scale = 1-((value-1600)/400);
			color = color_from_rgba(191*scale, 127*scale, 64);
		}
		else
			color = color_from_rgba(0, 0, 0);

		colors[c] = color;
	}
}
