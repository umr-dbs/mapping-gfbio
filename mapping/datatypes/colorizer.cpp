

#include "datatypes/colorizer.h"
#include "util/exceptions.h"
#include "util/make_unique.h"

#include <cmath>
#include <vector>
#include <utility>

uint32_t color_from_rgba(uint8_t r, uint8_t g, uint8_t b, uint8_t a) {
	return (uint32_t) a << 24 | (uint32_t) b << 16 | (uint32_t) g << 8 | (uint32_t) r;
}

static uint8_t r_from_color(uint32_t color) {
	return color & 0xff;
}

static uint8_t g_from_color(uint32_t color) {
	return (color & 0xff00) >> 8;
}

static uint8_t b_from_color(uint32_t color) {
	return (color & 0xff0000) >> 16;
}

static uint8_t a_from_color(uint32_t color) {
	return (color & 0xff000000) >> 24;
}

static uint8_t channel_from_double(double c) {
	if (c > 255.0)
		return 255;
	if (c < 0)
		return 0;
	return std::round(c);
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


class InterpolatingAbsoluteColorizer : public Colorizer {
	public:
		InterpolatingAbsoluteColorizer(const std::vector< std::pair<double,uint32_t> > &breakpoints) : Colorizer(true), breakpoints(breakpoints) {}
		~InterpolatingAbsoluteColorizer() {}
		virtual void fillPalette(uint32_t *colors, int num_colors, double min, double max) const;
	private:
		const std::vector< std::pair<double, uint32_t> > &breakpoints;
};

void InterpolatingAbsoluteColorizer::fillPalette(uint32_t *colors, int num_colors, double min, double max) const {
	double step = (num_colors > 1) ? ((max - min) / (num_colors-1)) : 0;
	for (int c = 0; c < num_colors; c++) {
		double value = min + c*step;
		uint32_t color;
		if (value <= breakpoints[0].first) {
			color = breakpoints[0].second;
		}
		else if (value >= breakpoints[breakpoints.size()-1].first) {
			color = breakpoints[breakpoints.size()-1].second;
		}
		else {
			for (int i=1;i<breakpoints.size();i++) {
				if (value <= breakpoints[i].first) {
					auto last_color = breakpoints[i-1].second;
					auto next_color = breakpoints[i].second;
					double fraction = (value-breakpoints[i-1].first) / (breakpoints[i].first - breakpoints[i-1].first);

					uint8_t r = channel_from_double(r_from_color(last_color) * (1-fraction) + r_from_color(next_color) * fraction);
					uint8_t g = channel_from_double(g_from_color(last_color) * (1-fraction) + g_from_color(next_color) * fraction);
					uint8_t b = channel_from_double(b_from_color(last_color) * (1-fraction) + b_from_color(next_color) * fraction);
					uint8_t a = channel_from_double(a_from_color(last_color) * (1-fraction) + a_from_color(next_color) * fraction);

					color = color_from_rgba(r, g, b, a);
					break;
				}
			}
		}

		colors[c] = color;
	}
}




class GreyscaleColorizer : public Colorizer {
	public:
		GreyscaleColorizer() : Colorizer(false) {}
		virtual ~GreyscaleColorizer() {};
		virtual void fillPalette(uint32_t *colors, int num_colors, double min, double max) const {
			for (int c = 0; c < num_colors; c++) {
				uint8_t color = (uint8_t) (255.0*c/num_colors);
				colors[c] = color_from_rgba(color, color, color, 255);
			}
		}
};



class HSVColorizer : public Colorizer {
	public:
		HSVColorizer() : Colorizer(false) {}
		virtual ~HSVColorizer() {};
		virtual void fillPalette(uint32_t *colors, int num_colors, double min, double max) const {
			for (int c = 0; c < num_colors; c++) {
				double frac = (double) c / num_colors;
				colors[c] = color_from_hsva((uint16_t) (120.0-(120.0*frac)), 150, 255);
			}
		}
};


static const std::vector< std::pair<double, uint32_t> > heatmap_breakpoints = {
	std::make_pair(  0, color_from_rgba(  0,   0, 255,  50)),
	std::make_pair(100, color_from_rgba(  0,   0, 255, 150)),
	std::make_pair(150, color_from_rgba(  0, 255, 255, 255)),
	std::make_pair(200, color_from_rgba(  0, 255,   0, 255)),
	std::make_pair(235, color_from_rgba(255, 255,   0, 255)),
	std::make_pair(255, color_from_rgba(255,   0,   0, 255))
};


static const std::vector< std::pair<double, uint32_t> > temperature_breakpoints = {
	std::make_pair(-50, color_from_rgba(  0,   0,   0)),
	std::make_pair(-30, color_from_rgba(255,   0, 255)),
	std::make_pair(-10, color_from_rgba(  0,   0, 255)),
	std::make_pair(  0, color_from_rgba(  0, 255, 255)),
	std::make_pair( 10, color_from_rgba(255, 255,   0)),
	std::make_pair( 30, color_from_rgba(255,   0,   0)),
	std::make_pair( 50, color_from_rgba(255, 255, 255))
};


static const std::vector< std::pair<double, uint32_t> > height_breakpoints = {
	std::make_pair(   0, color_from_rgba(170, 255, 170)), // #AAFFAA
	std::make_pair(1000, color_from_rgba(  0, 255,   0)), // #00FF00
	std::make_pair(1200, color_from_rgba(255, 255,   0)), // #FFFF00
	std::make_pair(1400, color_from_rgba(255, 127,   0)), // #FF7F00
	std::make_pair(1600, color_from_rgba(191, 127,  63)), // #BF7F3F
	std::make_pair(2000, color_from_rgba(  0,   0,   0)), // #000000
	std::make_pair(4000, color_from_rgba(255, 255, 255)), // #ffffff
	std::make_pair(8000, color_from_rgba(  0,   0, 255))  // #0000ff
};


static const std::vector< std::pair<double, uint32_t> > cpm_breakpoints = {
	std::make_pair(    0, color_from_rgba(  0, 255,   0)),
	std::make_pair(  100, color_from_rgba(200, 255,   0)),
	std::make_pair( 1000, color_from_rgba(255,   0,   0)),
	std::make_pair(10000, color_from_rgba(  0,   0,   0))
};



class GlobalLandCoverColorizer : public Colorizer {
	public:
		GlobalLandCoverColorizer() : Colorizer(true) {}
		virtual ~GlobalLandCoverColorizer() {}
		virtual void fillPalette(uint32_t *colors, int num_colors, double min, double max) const {
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
		};
};




std::unique_ptr<Colorizer> Colorizer::make(const std::string &name) {
	if (name == "hsv")
		return make_unique<HSVColorizer>();
	if (name == "heatmap")
		return make_unique<InterpolatingAbsoluteColorizer>(heatmap_breakpoints);
	if (name == "temperature")
		return make_unique<InterpolatingAbsoluteColorizer>(temperature_breakpoints);
	if (name == "height")
		return make_unique<InterpolatingAbsoluteColorizer>(height_breakpoints);
	if (name == "cpm")
		return make_unique<InterpolatingAbsoluteColorizer>(cpm_breakpoints);
	if (name == "glc")
		return make_unique<GlobalLandCoverColorizer>();
	return make_unique<GreyscaleColorizer>();
}
