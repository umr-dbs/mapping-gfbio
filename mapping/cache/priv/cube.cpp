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

Interval::Interval() : a(0), b(0) {
}

Interval::Interval(double a, double b) : a(a), b(b) {
}

Interval::Interval(const Interval& i) :
	a(i.a), b(i.b) {
}

Interval::Interval(BinaryStream& stream) {
	stream.read(&a);
	stream.read(&b);
}

bool Interval::empty() const {
	return a == 0 && b == 0;
}

bool Interval::intersects(const Interval& other) const {
	return !(a > other.b || b < other.a);
}

bool Interval::contains(const Interval& other) const {
	return a <= other.a && b >= other.b;
}

Interval Interval::combine(const Interval& other) const {
	return Interval( std::min(a, other.a), std::max(b, other.b) );
}

double Interval::distance() const {
	return b-a;
}

void Interval::toStream(BinaryStream& stream) const {
	stream.write(a);
	stream.write(b);
}

std::string Interval::to_string() const {
	return concat("[",a,", ",b,"]");
}

//
// Cube
//

template<int DIM>
Cube<DIM>::Cube() {
	std::fill_n( dims, DIM, Interval() );
}


template<int DIM>
Cube<DIM>::Cube(const Cube<DIM>& c) {
	for ( int i = 0; i < DIM; i++ ) {
		dims[i].a = c.dims[i].a;
		dims[i].b = c.dims[i].b;
	}
}

template<int DIM>
Cube<DIM>::Cube(BinaryStream& stream) {
	for ( int i = 0; i < DIM; i++ ) {
		dims[i] = Interval( stream );
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
double Cube<DIM>::volume() const {
	double res = 1.0;
	for ( int i = 0; i < DIM; i++ ) {
		res *= dims[i].distance();
	}
	return res;
}

template<int DIM>
Cube<DIM> Cube<DIM>::combine(const Cube<DIM>& other) const {
	Cube res;
	for ( int i = 0; i < DIM; i++ ) {
		res.set_dimension( i, dims[i].combine( other.dims[i] ) );
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
void Cube<DIM>::toStream(BinaryStream& stream) const {
	for ( int i = 0; i < DIM; i++ ) {
		dims[i].toStream(stream);
	}
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

template class Cube<2>;
template class Cube<3>;
