#ifndef PLOT_TEXT_H
#define PLOT_TEXT_H

#include <string>

#include "plot/plot.h"

class TextPlot : public GenericPlot {
	public:
		TextPlot(const std::string &text);
		virtual ~TextPlot();

		std::string toJSON();

	private:
		std::string text;
};

#endif
