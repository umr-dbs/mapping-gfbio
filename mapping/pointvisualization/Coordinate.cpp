#include "Coordinate.h"

using namespace pv;

Coordinate::Coordinate(double x, double y) : x(x), y(y) {}

double Coordinate::getX() const {
	return this->x;
}

double Coordinate::getY() const {
	return this->y;
}

