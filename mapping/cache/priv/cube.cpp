/*
 * Cube.cpp
 *
 *  Created on: 12.11.2015
 *      Author: mika
 */

#include "cache/priv/cube.h"
#include "util/exceptions.h"
#include "util/concat.h"
#include <sstream>
#include <limits>

///////////////////////////////////////////////////////////
//
// INTERVAL
//
///////////////////////////////////////////////////////////

Interval::Interval() : a(0), b(0) {
}

Interval::Interval(double a, double b) : a(a), b(b) {
}

Interval::Interval(BinaryReadBuffer& buffer) : a(buffer.read<double>()), b(buffer.read<double>()) {
}

bool Interval::empty() const {
	return a == 0 && b == 0;
}

bool Interval::intersects(const Interval& other) const {
	return a <= other.b && b >= other.a;
}

bool Interval::contains(const Interval& other) const {
	return a - std::numeric_limits<double>::epsilon() <= other.a &&
		   b + std::numeric_limits<double>::epsilon() >= other.b;
}

bool Interval::contains(double value) const {
	return a - std::numeric_limits<double>::epsilon() <= value &&
		   b + std::numeric_limits<double>::epsilon() >= value;
}

Interval Interval::combine(const Interval& other) const {
	return Interval( std::min(a, other.a), std::max(b, other.b) );
}

Interval Interval::intersect(const Interval& other) const {
	if ( !intersects(other) )
		throw ArgumentException("Cannot intersect disjunct intervals");
	return Interval( std::max(a, other.a), std::min(b, other.b) );
}


double Interval::distance() const {
	return b-a;
}

void Interval::serialize(BinaryWriteBuffer& buffer, bool) const {
	buffer << a << b;
}

bool Interval::operator ==(const Interval& o) const {
	return std::abs(a - o.a) < std::numeric_limits<double>::epsilon() &&
		   std::abs(b - o.b) < std::numeric_limits<double>::epsilon();
}

bool Interval::operator !=(const Interval& o) const {
	return !this->operator ==(o);
}

std::string Interval::to_string() const {
	return concat("[",a,", ",b,"]");
}

///////////////////////////////////////////////////////////
//
// POINT
//
///////////////////////////////////////////////////////////

template<int DIM>
Point<DIM>::Point() {
	values.fill(0);
}

template<int DIM>
double Point<DIM>::get_value(int dim) const {
	if ( dim < 0 || dim >= DIM )
		throw ArgumentException(concat("Cannot get value for dimension ",dim, " from point with ", DIM, " dimensions"));
	return values[dim];
}

template<int DIM>
void Point<DIM>::set_value(int dim, double value) {
	if ( dim < 0 || dim >= DIM )
		throw ArgumentException(concat("Cannot set value for dimension ",dim, " in point with ", DIM, " dimensions"));
	values[dim] = value;
}

template<int DIM>
bool Point<DIM>::operator ==(const Point<DIM>& o) const {
	bool res = true;
	for ( int i = 0; res && i < DIM; i++ ) {
		res &= std::abs(values[i] - o.values[i]) < std::numeric_limits<double>::epsilon();
	}
	return res;
}

template<int DIM>
bool Point<DIM>::operator !=(const Point<DIM>& o) const {
	return !(*this == o);
}

template<int DIM>
double Point<DIM>::distance_to(const Point<DIM>& o) const {
	Point<DIM> dif = this->operator -(o);
	double sqsum = 0;
	for ( int i = 0; i < DIM; i++ )
		sqsum += dif.values[i] * dif.values[i];
	return std::sqrt(sqsum);
}

template<int DIM>
Point<DIM> Point<DIM>::operator *(double s) const {
	Point<DIM> res;
	for ( int i = 0; i < DIM; i++ )
		res.values[i] = s*values[i];
	return res;
}

template<int DIM>
Point<DIM> Point<DIM>::operator -(const Point<DIM>& o) const {
	Point<DIM> res;
	for ( int i = 0; i < DIM; i++ )
		res.values[i] = values[i] - o.values[i];
	return res;
}

template<int DIM>
Point<DIM> Point<DIM>::operator +(const Point<DIM>& o) const {
	Point<DIM> res;
	for ( int i = 0; i < DIM; i++ )
		res.values[i] = values[i] + o.values[i];
	return res;
}

template<int DIM>
std::string Point<DIM>::to_string() const {
	std::ostringstream ss;
	ss << "Point: (";
	for ( int i = 0; i < DIM; i++ ) {
		if ( i > 0 )
			ss << ",";
		ss << values[i];
	}
	ss << ")";
	return ss.str();
}

///////////////////////////////////////////////////////////
//
// CUBE
//
///////////////////////////////////////////////////////////

template<int DIM>
Cube<DIM>::Cube() {
	dims.fill( Interval() );
}


template<int DIM>
Cube<DIM>::Cube(BinaryReadBuffer& buffer) {
	for ( int i = 0; i < DIM; i++ ) {
		dims[i] = Interval( buffer );
	}
}


template<int DIM>
const Interval& Cube<DIM>::get_dimension(int dim) const {
	if ( dim < 0 || dim >= DIM )
		throw ArgumentException(concat("Cannot get dimension ",dim, " from cube with ", DIM, " dimensions"));
	return dims[dim];
}

template<int DIM>
void Cube<DIM>::set_dimension(int dim, double a, double b) {
	if ( dim < 0 || dim >= DIM )
		throw ArgumentException(concat("Cannot set dimension ",dim, " from cube with ", DIM, " dimensions"));
	dims[dim].a = a;
	dims[dim].b = b;
}

template<int DIM>
void Cube<DIM>::set_dimension(int dim, const Interval& i) {
	set_dimension( dim, i.a, i.b );
}

template<int DIM>
bool Cube<DIM>::empty() const {
	bool res = true;
	for ( int i = 0; res && i < DIM; i++ ) {
		res &= dims[i].empty();
	}
	return res;
}

template<int DIM>
bool Cube<DIM>::intersects(const Cube<DIM>& other) const {
	bool res = true;
	for ( int i = 0; res && i < DIM; i++ ) {
		res &= dims[i].intersects( other.dims[i] );
	}
	return res;
}

template<int DIM>
bool Cube<DIM>::contains(const Cube<DIM>& other) const {
	bool res = true;
	for ( int i = 0; res && i < DIM; i++ ) {
		res &= dims[i].contains( other.dims[i] );
	}
	return res;
}

template<int DIM>
bool Cube<DIM>::operator ==(const Cube<DIM>& o) const {
	bool res = true;
	for ( int i = 0; res && i < DIM; i++ ) {
		res &= (dims[i] == o.dims[i]);
	}
	return res;
}

template<int DIM>
bool Cube<DIM>::operator !=(const Cube<DIM>& o) const {
	return !(*this == o);
}


template<int DIM>
double Cube<DIM>::volume() const {
	double res = 1.0;
	for ( int i = 0; i < DIM; i++ ) {
		res *= dims[i].distance();
	}
	return res;
}

template<int DIM>
Cube<DIM> Cube<DIM>::combine(const Cube<DIM>& other) const {
	Cube<DIM> res;
	for ( int i = 0; i < DIM; i++ ) {
		res.set_dimension( i, dims[i].combine( other.dims[i] ) );
	}
	return res;
}

template<int DIM>
Cube<DIM> Cube<DIM>::intersect(const Cube<DIM>& other) const {
	Cube<DIM> res;
	for ( int i = 0; i < DIM; i++ ) {
		res.set_dimension( i, dims[i].intersect( other.dims[i] ) );
	}
	return res;
}


template<int DIM>
std::vector<Cube<DIM> > Cube<DIM>::dissect_by(const Cube<DIM>& fill) const {
	std::vector<Cube<DIM>> res;

	if ( fill.contains(*this) )
		return res;
	else if ( !intersects(fill) )
		throw ArgumentException("Filling cube must intersect this cube for dissection");

	Cube<DIM> work = Cube<DIM>(*this);

	// Here we go!
	for ( int i = 0; i < DIM; i++ ) {
		Interval &my_dim = work.dims[i];
		const Interval &o_dim = fill.dims[i];

		// Left
		if ( o_dim.a > my_dim.a ) {
			Cube<DIM> rem( work );
			rem.set_dimension(i, my_dim.a, o_dim.a );
			res.push_back(rem);
			my_dim.a = o_dim.a;
		}

		// Right
		if ( o_dim.b < my_dim.b ) {
			Cube<DIM> rem( work );
			rem.set_dimension(i, o_dim.b, my_dim.b );
			res.push_back(rem);
			my_dim.b = o_dim.b;
		}
	}
	return res;
}

template<int DIM>
Point<DIM> Cube<DIM>::get_centre_of_mass() const {
	Point<DIM> res;
	for ( int i = 0; i < DIM; i++ ) {
		res.set_value(i, dims[i].a + (dims[i].distance() / 2) );
	}
	return res;
}

template<int DIM>
void Cube<DIM>::serialize(BinaryWriteBuffer& buffer, bool) const {
	for ( int i = 0; i < DIM; i++ ) {
		buffer.write(dims[i]);
	}
}

template<int DIM>
bool Cube<DIM>::contains(const Point<DIM>& p) const {
	for ( int i = 0; i < DIM; i++ ) {
		if ( !dims[i].contains(p.get_value(i)) )
			return false;
	}
	return true;
}

template<int DIM>
std::string Cube<DIM>::to_string() const {
	std::ostringstream ss;
	ss << "Cube: ";
	for ( int i = 0; i < DIM; i++ ) {
		if ( i > 0 )
			ss << "x";
		ss << dims[i].to_string();
	}
	return ss.str();
}

template class Point<2>;
template class Point<3> ;
template class Cube<2>;
template class Cube<3> ;
