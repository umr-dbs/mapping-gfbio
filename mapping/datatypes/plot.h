#ifndef PLOT_PLOT_H
#define PLOT_PLOT_H

#include "util/exceptions.h"
#include "util/binarystream.h"
#include "util/hash.h"

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

	virtual void serialize(BinaryWriteBuffer &buffer) const {
		// TODO
		throw MustNotHappenException("Implement me!");
	}

	static std::unique_ptr<GenericPlot> deserialize(BinaryReadBuffer &buffer) {
		// TODO
		throw MustNotHappenException("Implement me!");
	}

	std::string hash() const {
		std::string serialized = toJSON();

		return calculateHash((const unsigned char *) serialized.c_str(), (int) serialized.length()).asHex();
	}
};

#endif /* DATAVECTOR_H_ */
