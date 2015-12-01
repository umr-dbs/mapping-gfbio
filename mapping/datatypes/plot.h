#ifndef PLOT_PLOT_H
#define PLOT_PLOT_H

#include "util/exceptions.h"
#include "util/binarystream.h"

#include <string>
#include <memory>


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

	virtual std::unique_ptr<GenericPlot> clone() const {
		return std::unique_ptr<GenericPlot>();
	}

	virtual void toStream(BinaryStream &stream) const {
		// TODO
	}

	static std::unique_ptr<GenericPlot> fromStream(BinaryStream &stream) {
		// TODO
		throw OperatorException("Implement me!");
	}
};

#endif /* DATAVECTOR_H_ */
