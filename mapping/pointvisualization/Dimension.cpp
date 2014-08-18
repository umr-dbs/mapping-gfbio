#include "Dimension.h"

using namespace pv;

Dimension::Dimension(double width, double height) :
		width(width), height(height) {

}

double Dimension::getWidth() const {
	return this->width;
}

double Dimension::getHeight() const {
	return this->height;
}

Dimension Dimension::halve() const {
	return Dimension(width / 2, height / 2);
}
