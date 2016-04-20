

#include "datatypes/colorizer.h"
#include "datatypes/unit.h"
#include "util/exceptions.h"
#include "util/make_unique.h"

#include <cmath>
#include <vector>
#include <iomanip>

color_t color_from_rgba(uint8_t r, uint8_t g, uint8_t b, uint8_t a) {
	return (uint32_t) a << 24 | (uint32_t) b << 16 | (uint32_t) g << 8 | (uint32_t) r;
}

static uint8_t r_from_color(color_t color) {
	return color & 0xff;
}

static uint8_t g_from_color(color_t color) {
	return (color & 0xff00) >> 8;
}

static uint8_t b_from_color(color_t color) {
	return (color & 0xff0000) >> 16;
}

static uint8_t a_from_color(color_t color) {
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
color_t color_from_hsva(uint16_t h, uint8_t s, uint8_t v, uint8_t a) {
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



Colorizer::Colorizer(const ColorTable &table, Interpolation interpolation) : table(table), interpolation(interpolation) {
}

Colorizer::~Colorizer() {
}

OwningColorizer::OwningColorizer() : Colorizer(table) {
}

OwningColorizer::~OwningColorizer() {
}


void Colorizer::fillPalette(color_t *colors, int num_colors, double min, double max) const {
	double step = (num_colors > 1) ? ((max - min) / (num_colors-1)) : 0;
	for (int c = 0; c < num_colors; c++) {
		double value = min + c*step;
		color_t color;
		if (value <= table[0].value) {
			color = table[0].color;
		}
		else if (value >= table[table.size()-1].value) {
			color = table[table.size()-1].color;
		}
		else {
			color = color_from_rgba(0, 0, 0, 0);
			for (size_t i=1;i<table.size();i++) {
				if (value <= table[i].value) {
					auto last_color = table[i-1].color;
					auto next_color = table[i].color;

					if (interpolation == Interpolation::LINEAR) {
						double fraction = (value-table[i-1].value) / (table[i].value - table[i-1].value);

						uint8_t r = channel_from_double(r_from_color(last_color) * (1-fraction) + r_from_color(next_color) * fraction);
						uint8_t g = channel_from_double(g_from_color(last_color) * (1-fraction) + g_from_color(next_color) * fraction);
						uint8_t b = channel_from_double(b_from_color(last_color) * (1-fraction) + b_from_color(next_color) * fraction);
						uint8_t a = channel_from_double(a_from_color(last_color) * (1-fraction) + a_from_color(next_color) * fraction);

						color = color_from_rgba(r, g, b, a);
					}
					else if (interpolation == Interpolation::NEAREST) {
						if (value-table[i-1].value > table[i].value-value)
							color = table[i-1].color;
						else
							color = table[i].color;
					}
					else {
						throw MustNotHappenException("Unknown interpolation mode in colorizer");
					}
					break;
				}
			}
		}

		colors[c] = color;
	}
}

static void color_as_html(std::ostream &s, color_t color) {
	if (a_from_color(color) == 255) {
		s << "#";
		s << std::setfill('0') << std::hex;
		s << std::setw(2) << (int) r_from_color(color)
		  << std::setw(2) << (int) g_from_color(color)
		  << std::setw(2) << (int) b_from_color(color);
		s << std::setw(0) << std::dec;
	}
	else {
		s << "rgba(" << (int) r_from_color(color) << "," << (int) g_from_color(color) << "," << (int) b_from_color(color) << "," << (a_from_color(color)/255.0) << ")";
	}
}

std::string Colorizer::toJson() const {
	std::ostringstream ss;

	ss << "{ \"interpolation\": \"";
	if (interpolation == Interpolation::LINEAR)
		ss << "linear";
	else if (interpolation == Interpolation::NEAREST)
		ss << "nearest";
	ss << "\", \"breakpoints\": [\n";
	for (size_t i=0;i<table.size();i++) {
		if (i != 0)
			ss << ",\n";
		ss << "[" << table[i].value << ",\"";
		color_as_html(ss, table[i].color);
		ss << "\"]";
	}
	ss << "]}";

	return ss.str();
}


static const Colorizer::ColorTable heatmap_breakpoints = {
	Colorizer::Breakpoint(  0, color_from_rgba(  0,   0, 255,  50)),
	Colorizer::Breakpoint(100, color_from_rgba(  0,   0, 255, 150)),
	Colorizer::Breakpoint(150, color_from_rgba(  0, 255, 255, 255)),
	Colorizer::Breakpoint(200, color_from_rgba(  0, 255,   0, 255)),
	Colorizer::Breakpoint(235, color_from_rgba(255, 255,   0, 255)),
	Colorizer::Breakpoint(255, color_from_rgba(255,   0,   0, 255))
};


static const Colorizer::ColorTable temperature_breakpoints = {
	Colorizer::Breakpoint(-50, color_from_rgba(  0,   0,   0)),
	Colorizer::Breakpoint(-30, color_from_rgba(255,   0, 255)),
	Colorizer::Breakpoint(-10, color_from_rgba(  0,   0, 255)),
	Colorizer::Breakpoint(  0, color_from_rgba(  0, 255, 255)),
	Colorizer::Breakpoint( 10, color_from_rgba(255, 255,   0)),
	Colorizer::Breakpoint( 30, color_from_rgba(255,   0,   0)),
	Colorizer::Breakpoint( 50, color_from_rgba(255, 255, 255))
};


static const Colorizer::ColorTable height_breakpoints = {
	Colorizer::Breakpoint(   0, color_from_rgba(170, 255, 170)), // #AAFFAA
	Colorizer::Breakpoint(1000, color_from_rgba(  0, 255,   0)), // #00FF00
	Colorizer::Breakpoint(1200, color_from_rgba(255, 255,   0)), // #FFFF00
	Colorizer::Breakpoint(1400, color_from_rgba(255, 127,   0)), // #FF7F00
	Colorizer::Breakpoint(1600, color_from_rgba(191, 127,  63)), // #BF7F3F
	Colorizer::Breakpoint(2000, color_from_rgba(  0,   0,   0)), // #000000
	Colorizer::Breakpoint(4000, color_from_rgba(255, 255, 255)), // #ffffff
	Colorizer::Breakpoint(8000, color_from_rgba(  0,   0, 255))  // #0000ff
};


static const Colorizer::ColorTable cpm_breakpoints = {
	Colorizer::Breakpoint(    0, color_from_rgba(  0, 255,   0)),
	Colorizer::Breakpoint(  100, color_from_rgba(200, 255,   0)),
	Colorizer::Breakpoint( 1000, color_from_rgba(255,   0,   0)),
	Colorizer::Breakpoint(10000, color_from_rgba(  0,   0,   0))
};

static const Colorizer::ColorTable error_breakpoints = {
	Colorizer::Breakpoint(1, color_from_rgba(255, 0, 0))
};

// See http://forobs.jrc.ec.europa.eu/products/glc2000/legend.php
static const Colorizer::ColorTable glc2000_breakpoints = {
	Colorizer::Breakpoint( 0, color_from_rgba(0, 0, 0, 0)), // invalid value
	Colorizer::Breakpoint( 1, color_from_rgba(0, 100, 0)),
	Colorizer::Breakpoint( 2, color_from_rgba(0, 150, 0)),
	Colorizer::Breakpoint( 3, color_from_rgba(175, 255, 98)),
	Colorizer::Breakpoint( 4, color_from_rgba(139, 68, 18)),
	Colorizer::Breakpoint( 5, color_from_rgba(205, 126, 95)),
	Colorizer::Breakpoint( 6, color_from_rgba(140, 190, 0)),
	Colorizer::Breakpoint( 7, color_from_rgba(119, 150, 255)),
	Colorizer::Breakpoint( 8, color_from_rgba(0, 70, 200)),
	Colorizer::Breakpoint( 9, color_from_rgba(0, 230, 0)),
	Colorizer::Breakpoint(10, color_from_rgba(0, 0, 0)),
	Colorizer::Breakpoint(11, color_from_rgba(255, 118, 0)),
	Colorizer::Breakpoint(12, color_from_rgba(255, 179, 0)),
	Colorizer::Breakpoint(13, color_from_rgba(255, 234, 158)),
	Colorizer::Breakpoint(14, color_from_rgba(222, 202, 161)),
	Colorizer::Breakpoint(15, color_from_rgba(0, 150, 150)),
	Colorizer::Breakpoint(16, color_from_rgba(255, 224, 229)),
	Colorizer::Breakpoint(17, color_from_rgba(255, 116, 232)),
	Colorizer::Breakpoint(18, color_from_rgba(202, 138, 255)),
	Colorizer::Breakpoint(19, color_from_rgba(180, 180, 180)),
	Colorizer::Breakpoint(20, color_from_rgba(138, 227, 255)),
	Colorizer::Breakpoint(21, color_from_rgba(240, 240, 240)),
	Colorizer::Breakpoint(22, color_from_rgba(255, 0, 0)),
	Colorizer::Breakpoint(23, color_from_rgba(0, 0, 0, 0)) // invalid value
};


std::unique_ptr<Colorizer> Colorizer::fromUnit(const Unit &unit) {
	if (unit.getMeasurement() == "temperature" && unit.getUnit() == "c")
		return make_unique<Colorizer>(temperature_breakpoints);
	if (unit.getMeasurement() == "elevation" && unit.getUnit() == "m")
		return make_unique<Colorizer>(height_breakpoints);
	if (unit.getMeasurement() == "frequency" && unit.getUnit() == "heatmap")
		return make_unique<Colorizer>(heatmap_breakpoints);
	if (unit.getMeasurement() == "radiation" && unit.getUnit() == "cpm")
		return make_unique<Colorizer>(cpm_breakpoints);
	if (unit.getUnit() == "errormessage")
		return make_unique<Colorizer>(error_breakpoints, Interpolation::NEAREST);
	// TODO: we need a better default colorizer, optimized for contrast
	if (unit.getUnit() == "classification")
		return make_unique<Colorizer>(glc2000_breakpoints, Interpolation::NEAREST);

	// TODO: if we have a min/max, we can do a proper scaling. If we don't, what do we do?
	if (!unit.hasMinMax())
		throw ArgumentException("Cannot create a suitable Colorizer for the given Unit.");

	auto c = make_unique<OwningColorizer>();
	c->table.emplace_back(unit.getMin(), color_from_rgba(0,0,0,255));
	c->table.emplace_back(unit.getMax(), color_from_rgba(255,255,255,255));
	return std::unique_ptr<Colorizer>(c.release());
}
