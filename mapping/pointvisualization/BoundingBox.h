#ifndef POINTVISUALIZATION_BOUNDINGBOX_H_
#define POINTVISUALIZATION_BOUNDINGBOX_H_

#include <cmath>
#include <sstream>
#include <string>

#include "Coordinate.h"
#include "Dimension.h"
#include "Circle.h"

namespace pv {

	/**
	 * A bounding box consisting of a center and a half-dimension.
	 */
	class BoundingBox {
		public:
			/**
			 * Constructs a BoundingBox using a coordinate center, a half dimension and an epsilon distance.
			 * @param center
			 * @param halfDimension
			 * @param epsilonDistance Minimum circle distance, used for calculating intersects and contains.
			 */
			BoundingBox(Coordinate center, Dimension halfDimension, double epsilonDistance);

			/**
			 * Calculate the intersection between the bounding box and a circle.
			 * (Takes epsilonDistance into account.)
			 * @param circle
			 * @return true if the circle intersects the box, false else.
			 */
			bool intersects(Circle& circle) const;
			/**
			 * Calculate the containment between the bounding box and a circle.
			 * (Takes epsilonDistance into account.)
			 * @param circle
			 * @return true if the box fully contains the circle, false else.
			 */
			bool contains(Circle& circle) const;

			/**
			 * @return center
			 */
			Coordinate getCenter() const;
			/**
			 * @return half-dimension
			 */
			Dimension getHalfDimension() const;
			/**
			 * @return epsilonDistance between two circles
			 */
			double getEpsilonDistance() const;

			/**
			 * String representation of the bounding box.
			 */
			std::string to_string() const;
		private:
			const Coordinate center;
			const Dimension halfDimension;
			const double EPSILON_DISTANCE;
	};

}

#endif /* POINTVISUALIZATION_BOUNDINGBOX_H_ */
