#ifndef PLOT_PLOT_H
#define PLOT_PLOT_H

#include <string>

/**
 * Abstract base class for all output data vector types.
 */
class GenericPlot {
public:
	virtual ~GenericPlot() {};

	/**
	 * Creates a JSON representation of the data vector.
	 */
	virtual std::string toJSON() = 0;
};

#endif /* DATAVECTOR_H_ */
