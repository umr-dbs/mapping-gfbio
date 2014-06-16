#ifndef RASTER_COLORS_H
#define RASTER_COLORS_H

#include <stdint.h>

uint32_t color_from_rgba(uint8_t r, uint8_t g, uint8_t b, uint8_t a = 255);

uint32_t color_from_hsva(uint16_t h, uint8_t s, uint8_t v, uint8_t a = 255);

class Colorizer {
	protected:
		Colorizer();
	public:
		virtual ~Colorizer();
		void setRange(uint32_t range) { this->range = range; }
		uint32_t getRange(uint32_t range) { return range; }
		virtual uint32_t colorize(uint32_t data) = 0;
	protected:
		uint32_t range;
};


class GreyscaleColorizer : public Colorizer {
	public:
		GreyscaleColorizer();
		virtual ~GreyscaleColorizer();
		virtual uint32_t colorize(uint32_t data);
};


class HSVColorizer : public Colorizer {
	public:
		HSVColorizer();
		virtual ~HSVColorizer();
		virtual uint32_t colorize(uint32_t data);
};

#endif
