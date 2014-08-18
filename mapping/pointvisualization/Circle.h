#ifndef POINTVISUALIZATION_CIRCLE_H_
#define POINTVISUALIZATION_CIRCLE_H_

#include <vector>
#include <memory>
#include <cmath>
#include <string>
#include <sstream>

#include "Coordinate.h"

namespace pv {

	/**
	 * A circle cluster consisting of a center, a radius and a number of points.
	 * The radius calculates of the minimum radius and the number of points (cluster).
	 */
	class Circle {
		public:
			/**
			 * Constructs a circle given a coordinate center, a minimum radius and an epsilon distance.
			 * @param circleMinRadius Minimum radius independent of the number of points.
			 * @param epsilonDistance Minimum distance between two points.
			 */
			Circle(Coordinate center, double circleMinRadius, double epsilonDistance);
			/**
			 * Constructs a circle given a coordinate center, a minimum radius and an epsilon distance.
			 * @param circleMinRadius Minimum radius independent of the number of points.
			 * @param epsilonDistance Minimum distance between two points.
			 * @param numberOfPoints Number of points in the cluster.
			 */
			Circle(Coordinate center, double circleMinRadius, double epsilonDistance, int numberOfPoints);

			/**
			 * Merge this circle with another one and construct a new one.
			 * @param circle
			 * @return merged circle
			 */
			Circle merge(Circle& circle) const;

			/**
			 * @return center
			 */
			Coordinate getCenter() const;
			/**
			 * @return x
			 */
			double getX() const;
			/**
			 * @return y
			 */
			double getY() const;
			/**
			 * @return radius
			 */
			double getRadius() const;
			/**
			 * @return number of points
			 */
			int getNumberOfPoints() const;

			/**
			 * Checks for intersection of this circle with another one.
			 * (Takes epsilonDistance into account.)
			 * @return true if circle intersect, false else.
			 */
			bool intersects(Circle& circle) const;

			/**
			 * String representation of circle.
			 */
			std::string to_string() const;

		private:
			Coordinate center;
			double radius;

			const double CIRCLE_MIN_RADIUS;
			const double EPSILON_DISTANCE;

			int numberOfPoints;

			double calculateRadius(int numberOfPoints) const;
	};

}

#endif /* POINTVISUALIZATION_CIRCLE_H_ */
