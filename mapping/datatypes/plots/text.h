#ifndef PLOT_TEXT_H
#define PLOT_TEXT_H

#include <string>

#include "datatypes/plot.h"

/**
 * This plot outputs text encapuslated in JSON
 */
class TextPlot : public GenericPlot {
	public:
		TextPlot(const std::string &text);
		virtual ~TextPlot();

		const std::string toJSON() const;

		std::unique_ptr<GenericPlot> clone() const {
			return std::unique_ptr<GenericPlot>();
		}

	private:
		std::string text;
};

#endif
