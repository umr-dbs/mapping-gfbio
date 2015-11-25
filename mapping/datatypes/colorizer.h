#ifndef DATATYPES_COLORIZER_H
#define DATATYPES_COLORIZER_H

#include <stdint.h>
#include <memory>

uint32_t color_from_rgba(uint8_t r, uint8_t g, uint8_t b, uint8_t a = 255);

uint32_t color_from_hsva(uint16_t h, uint8_t s, uint8_t v, uint8_t a = 255);

class Unit;

class Colorizer {
	protected:
		Colorizer(bool is_absolute);
	public:
		virtual ~Colorizer();
		virtual void fillPalette(uint32_t *colors, int num_colors, double min, double max) const = 0;
		const bool is_absolute;

		static std::unique_ptr<Colorizer> create(const std::string &name);
		static std::unique_ptr<Colorizer> fromUnit(const Unit &unit);
};

#endif
