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
	virtual const std::string toJSON() const = 0;

	virtual std::unique_ptr<GenericPlot> clone() const = 0;

	virtual void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const {
		// TODO
		throw MustNotHappenException("Implement me!");
	}

	static std::unique_ptr<GenericPlot> deserialize(BinaryReadBuffer &buffer) {
		// TODO
		throw MustNotHappenException("Implement me!");
	}
};

#endif /* DATAVECTOR_H_ */
