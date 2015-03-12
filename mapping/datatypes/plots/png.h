#ifndef PLOT_PNG_H
#define PLOT_PNG_H

#include <string>

#include "datatypes/plot.h"

class PNGPlot : public GenericPlot {
	public:
		PNGPlot(const std::string &binary);
		virtual ~PNGPlot();

		std::string toJSON();

	private:
		std::string binary;
};

#endif
