#include "Circle.h"

using namespace pv;

Circle::Circle(Coordinate center, double circleMinRadius, double epsilonDistance)
				: center(center), CIRCLE_MIN_RADIUS(circleMinRadius), EPSILON_DISTANCE(epsilonDistance), numberOfPoints(1) {
	radius = calculateRadius(1);
}

Circle::Circle(Coordinate center, double circleMinRadius, double epsilonDistance, int numberOfPoints) 
				: center(center), CIRCLE_MIN_RADIUS(circleMinRadius), EPSILON_DISTANCE(epsilonDistance), numberOfPoints(numberOfPoints) {
	radius = calculateRadius(numberOfPoints);
}

Circle Circle::merge(Circle& circle) const {
	int newWeight = numberOfPoints + circle.numberOfPoints;
	Coordinate newCenter = Coordinate((center.getX() * numberOfPoints + circle.center.getX() * circle.numberOfPoints) / newWeight, 
									  (center.getY() * numberOfPoints + circle.center.getY() * circle.numberOfPoints) / newWeight);
	return Circle(newCenter, CIRCLE_MIN_RADIUS, EPSILON_DISTANCE, newWeight);
}

Coordinate Circle::getCenter() const {
	return this->center;
}
double Circle::getX() const {
	return this->center.getX();
}
double Circle::getY() const {
	return this->center.getY();
}
double Circle::getRadius() const {
	return this->radius;
}

int Circle::getNumberOfPoints() const {
	return numberOfPoints;
}

bool Circle::intersects(Circle& circle) const {
	return sqrt(pow(center.getX() - circle.getCenter().getX(), 2)	+ pow(center.getY() - circle.getCenter().getY(), 2)) < (radius + circle.getRadius() + EPSILON_DISTANCE);
}

double Circle::calculateRadius(int numberOfPoints) const {
	return CIRCLE_MIN_RADIUS + log(numberOfPoints);
}

std::string Circle::to_string() const {
	std::stringstream toString;
	toString << "Center <" << center.getX() << ", " << center.getY() << ">, radius=" << radius << "]";
	return toString.str();
}
