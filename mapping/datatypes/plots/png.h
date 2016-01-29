#ifndef PLOT_PNG_H
#define PLOT_PNG_H

#include <string>

#include "datatypes/plot.h"

class PNGPlot : public GenericPlot {
	public:
		PNGPlot(const std::string &binary);
		virtual ~PNGPlot();

		const std::string toJSON() const;

		std::unique_ptr<GenericPlot> clone() const {
			return std::unique_ptr<GenericPlot>();
		}

	private:
		std::string binary;
};

#endif
