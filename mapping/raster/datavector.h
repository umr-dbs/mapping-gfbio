#ifndef DATAVECTOR_H_
#define DATAVECTOR_H_

#include <string>

/**
 * Abstract base class for all output data vector types.
 */
class DataVector {
public:
	virtual ~DataVector() {};

	/**
	 * Creates a JSON representation of the data vector.
	 */
	virtual std::string toJSON() = 0;
};

#endif /* DATAVECTOR_H_ */
