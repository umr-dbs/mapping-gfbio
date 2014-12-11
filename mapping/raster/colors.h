#ifndef RASTER_COLORS_H
#define RASTER_COLORS_H

#include <stdint.h>
#include <memory>

uint32_t color_from_rgba(uint8_t r, uint8_t g, uint8_t b, uint8_t a = 255);

uint32_t color_from_hsva(uint16_t h, uint8_t s, uint8_t v, uint8_t a = 255);

class Colorizer {
	protected:
		Colorizer(bool is_absolute);
	public:
		virtual ~Colorizer();
		virtual void fillPalette(uint32_t *colors, int num_colors, double min, double max) const = 0;
		const bool is_absolute;

		static std::unique_ptr<Colorizer> make(const std::string &name);
};


class GreyscaleColorizer : public Colorizer {
	public:
		GreyscaleColorizer();
		virtual ~GreyscaleColorizer();
		virtual void fillPalette(uint32_t *colors, int num_colors, double min, double max)  const;
};


class HSVColorizer : public Colorizer {
	public:
		HSVColorizer();
		virtual ~HSVColorizer();
		virtual void fillPalette(uint32_t *colors, int num_colors, double min, double max)  const;
};

class TemperatureColorizer : public Colorizer {
	public:
		TemperatureColorizer();
		virtual ~TemperatureColorizer();
		virtual void fillPalette(uint32_t *colors, int num_colors, double min, double max)  const;
};

class HeightColorizer : public Colorizer {
	public:
		HeightColorizer();
		virtual ~HeightColorizer();
		virtual void fillPalette(uint32_t *colors, int num_colors, double min, double max)  const;
};

class GlobalLandCoverColorizer : public Colorizer {
	public:
		GlobalLandCoverColorizer();
		virtual ~GlobalLandCoverColorizer();
		virtual void fillPalette(uint32_t *colors, int num_colors, double min, double max)  const;
};

#endif
