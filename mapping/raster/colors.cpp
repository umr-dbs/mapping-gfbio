

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
	if (name == "heatmap")
		return std::make_unique<HeatmapColorizer>();
	if (name == "temperature")
		return std::make_unique<TemperatureColorizer>();
	if (name == "height")
		return std::make_unique<HeightColorizer>();
	if (name == "cpm")
		return std::make_unique<CPMColorizer>();
	if (name == "glc")
		return std::make_unique<GlobalLandCoverColorizer>();
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



HeatmapColorizer::HeatmapColorizer() : Colorizer(false) {
}

HeatmapColorizer::~HeatmapColorizer() {
}

void HeatmapColorizer::fillPalette(uint32_t *colors, int num_colors, double, double) const {
	for (int c = 0; c < num_colors; c++) {
		int f = floor((double) c / num_colors * 256);
		uint8_t alpha = 255;
		if (f < 100)
			colors[c] = color_from_rgba(0, 0, 255, 50+f);
		else if (f < 150)
			colors[c] = color_from_rgba(0, 255-5*(149-f), 255, alpha);
		else if (f < 200)
			colors[c] = color_from_rgba(0, 255, 5*(199-f), alpha);
		else if (f < 235)
			colors[c] = color_from_rgba(255-8*(234-f), 255, 0, alpha);
		else
			colors[c] = color_from_rgba(f, 12*(255-f), 0, alpha);
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
	double step = (num_colors > 1) ? ((max - min) / (num_colors-1)) : 0;
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
		else if (value <= 4000) { // #ffffff
			double scale = 255*((value-2000)/2000);
			color = color_from_rgba(scale, scale, scale);
		}
		else if (value <= 8000) { // #0000ff
			double scale = 255*((value-4000)/4000);
			color = color_from_rgba(255-scale, 255-scale, 255);
		}
		else {
			color = color_from_rgba(0, 0, 255);
		}

		colors[c] = color;
	}
}


CPMColorizer::CPMColorizer() : Colorizer(true) {
}

CPMColorizer::~CPMColorizer() {
}

void CPMColorizer::fillPalette(uint32_t *colors, int num_colors, double min, double max) const {
	double step = (num_colors > 1) ? ((max - min) / (num_colors-1)) : 0;
	for (int c = 0; c < num_colors; c++) {
		double value = min + c*step;
		uint32_t color;
		if (value <= 100)
			color = color_from_rgba(2*value,255,0);
		else if (value <= 1000) {
			double d = (value-100)/900;
			color = color_from_rgba(200+d*55,255-d*255,0);
		}
		else if (value <= 10000) {
			double d = (value-1000)/9000;
			color = color_from_rgba(255-d*255,0,0);
		}
		else
			color = color_from_rgba(0,0,0);

		colors[c] = color;
	}
}

GlobalLandCoverColorizer::GlobalLandCoverColorizer() : Colorizer(true) {
}

GlobalLandCoverColorizer::~GlobalLandCoverColorizer() {
}

void GlobalLandCoverColorizer::fillPalette(uint32_t *colors, int num_colors, double min, double max) const {
	double step = (num_colors > 1) ? ((max - min) / (num_colors-1)) : 0;
	for (int c = 0; c < num_colors; c++) {
		int value = std::round(min + c*step);
		uint32_t color;
		if (value == 1)
			color = color_from_rgba(0, 100, 0);
		else if (value == 2)
			color = color_from_rgba(0, 150, 0);
		else if (value == 3)
			color = color_from_rgba(175, 255, 98);
		else if (value == 4)
			color = color_from_rgba(139, 68, 18);
		else if (value == 5)
			color = color_from_rgba(205, 126, 95);
		else if (value == 6)
			color = color_from_rgba(140, 190, 0);
		else if (value == 7)
			color = color_from_rgba(119, 150, 255);
		else if (value == 8)
			color = color_from_rgba(0, 70, 200);
		else if (value == 9)
			color = color_from_rgba(0, 230, 0);
		else if (value == 10)
			color = color_from_rgba(0, 0, 0);
		else if (value == 11)
			color = color_from_rgba(255, 118, 0);
		else if (value == 12)
			color = color_from_rgba(255, 179, 0);
		else if (value == 13)
			color = color_from_rgba(255, 234, 158);
		else if (value == 14)
			color = color_from_rgba(222, 202, 161);
		else if (value == 15)
			color = color_from_rgba(0, 150, 150);
		else if (value == 16)
			color = color_from_rgba(255, 224, 229);
		else if (value == 17)
			color = color_from_rgba(255, 116, 232);
		else if (value == 18)
			color = color_from_rgba(202, 138, 255);
		else if (value == 19)
			color = color_from_rgba(180, 180, 180);
		else if (value == 20)
			color = color_from_rgba(138, 227, 255);
		else if (value == 21)
			color = color_from_rgba(240, 240, 240);
		else if (value == 22)
			color = color_from_rgba(255, 0, 0);
		else if (value == 23)
			color = color_from_rgba(255, 255, 255);
		else
			color = color_from_rgba(0, 0, 255);

		colors[c] = color;
	}
}
