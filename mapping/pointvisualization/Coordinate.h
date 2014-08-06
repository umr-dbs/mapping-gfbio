#ifndef POINTVISUALIZATION_COORDINATE_H_
#define POINTVISUALIZATION_COORDINATE_H_

#include "Dimension.h"

namespace pv {

	/**
	 * A 2D-coordinate.
	 */
	class Coordinate {
		public:
			/**
			 * Constructs a coordinate given x and y location.
			 * @param x
			 * @param y
			 */
			Coordinate(double x, double y);
			/**
			 * @return x coordinate
			 */
			double getX() const;
			/**
			 * @return y coordinate
			 */
			double getY() const;

		private:
			const double x;
			const double y;
	};

}

#endif /* POINTVISUALIZATION_COORDINATE_H_ */
