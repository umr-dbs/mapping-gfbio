#include "BoundingBox.h"

using namespace pv;

BoundingBox::BoundingBox(Coordinate center, Dimension halfDimension, double epsilonDistance) : center(center), halfDimension(halfDimension), EPSILON_DISTANCE(epsilonDistance) {}

/**
 * http://stackoverflow.com/questions/401847/circle-rectangle-collision-detection-intersection
 */
bool BoundingBox::intersects(Circle& circle) const {
	auto circleDistance = std::make_pair(
			abs(circle.getX() - center.getX()),
			abs(circle.getY() - center.getY()));

	if (circleDistance.first
			> (halfDimension.getWidth() + circle.getRadius() + EPSILON_DISTANCE)) {
		return false;
	}
	if (circleDistance.second
			> (halfDimension.getHeight() + circle.getRadius() + EPSILON_DISTANCE)) {
		return false;
	}

	if (circleDistance.first <= halfDimension.getWidth()) {
		return true;
	}
	if (circleDistance.second <= halfDimension.getHeight()) {
		return true;
	}

	auto cornerDistance_sq = pow(
			circleDistance.first - halfDimension.getWidth(), 2)
			+ pow(circleDistance.second - halfDimension.getHeight(), 2);

	return (cornerDistance_sq <= pow(circle.getRadius(), 2));

	return true;
}

bool BoundingBox::contains(Circle& circle) const {
	return ( abs(circle.getX() - center.getX()) <= (halfDimension.getWidth() - circle.getRadius() - EPSILON_DISTANCE) )
		&& (abs(circle.getY() - center.getY()) <= (halfDimension.getHeight() - circle.getRadius() - EPSILON_DISTANCE));
}

Coordinate BoundingBox::getCenter() const {
	return center;
}

Dimension BoundingBox::getHalfDimension() const {
	return halfDimension;
}

double BoundingBox::getEpsilonDistance() const {
	return EPSILON_DISTANCE;
}

std::string BoundingBox::to_string() const {
	std::stringstream toString;
	toString << "X <" << center.getX() - halfDimension.getWidth() << ", " << center.getX() + halfDimension.getWidth() << ">, ";
	toString << "Y <" << center.getY() - halfDimension.getHeight() << ", " << center.getY() + halfDimension.getHeight() << ">, ";
	return toString.str();
}