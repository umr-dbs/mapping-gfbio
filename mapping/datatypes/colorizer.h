#ifndef DATATYPES_COLORIZER_H
#define DATATYPES_COLORIZER_H

#include <memory>
#include <vector>

#include <stdint.h>

using color_t = uint32_t;

color_t color_from_rgba(uint8_t r, uint8_t g, uint8_t b, uint8_t a = 255);
color_t color_from_hsva(uint16_t h, uint8_t s, uint8_t v, uint8_t a = 255);

class Unit;

/*
 * This is a basic Colorizer, based on a table of value:color pairs.
 * The color of a pixel is determined by interpolating between the nearest Breakpoints.
 *
 * This base class only carries a non-owning reference to the color table. Use OwningColorizer for vector management.
 */
class Colorizer {
	public:
		struct Breakpoint {
			Breakpoint(double v, color_t c) : value(v), color(c) {}
			double value;
			color_t color;
		};
		enum class Interpolation {
			NEAREST,
			LINEAR
		};
		using ColorTable = std::vector<Breakpoint>;

		Colorizer(const ColorTable &table, Interpolation interpolation = Interpolation::LINEAR);
		virtual ~Colorizer();

		void fillPalette(color_t *colors, int num_colors, double min, double max) const;

		static std::unique_ptr<Colorizer> fromUnit(const Unit &unit);
	private:
		static std::unique_ptr<Colorizer> create(const std::string &name);
		const ColorTable &table;
		Interpolation interpolation;
};

class OwningColorizer : public Colorizer {
	public:
		OwningColorizer();
		virtual ~OwningColorizer();
		ColorTable table;
};

#endif
