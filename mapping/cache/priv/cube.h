/*
 * Cube.h
 *
 *  Created on: 12.11.2015
 *      Author: mika
 */

#ifndef CUBE_H_
#define CUBE_H_

#include "util/binarystream.h"

#include <vector>
#include <string>
#include <cmath>
#include <array>

class Interval;
template <int DIM> class Point;
template <int DIM> class Cube;

class Interval {
public:
	Interval();
	Interval( double a, double b );
	Interval( BinaryStream &stream );
	bool empty() const;
	bool intersects( const Interval &other ) const;
	bool contains( const Interval &other ) const;
	bool contains( double value ) const;

	bool operator==( const Interval &o ) const;
	bool operator!=( const Interval &o ) const;

	Interval combine( const Interval &other ) const;
	Interval intersect( const Interval & other ) const;
	double distance() const;

	void toStream( BinaryStream &stream ) const;
	std::string to_string() const;

	double a, b;
};

template <int DIM>
class Point {
	friend class Cube<DIM>;
public:
	Point();
	double get_value( int dim ) const;
	void set_value( int dim, double value );

	bool operator==( const Point<DIM> &o ) const;
	bool operator!=( const Point<DIM> &o ) const;

	std::string to_string() const;
private:
	std::array<double,DIM> values;
};

template <int DIM>
class Cube {
public:
	Cube();
	Cube( BinaryStream &stream );
	const Interval& get_dimension( int dim ) const;
	void set_dimension( int dim, double a, double b );

	bool empty() const;
	bool intersects( const Cube<DIM> &other ) const;
	bool contains( const Cube<DIM> &other ) const;
	bool contains( const Point<DIM> &p ) const;

	bool operator==( const Cube<DIM> &o ) const;
	bool operator!=( const Cube<DIM> &o ) const;

	double volume() const;
	Cube<DIM> combine( const Cube<DIM> &other ) const;
	Cube<DIM> intersect( const Cube<DIM> &other ) const;
	Point<DIM> get_centre_of_mass() const;

	std::vector<Cube<DIM>> dissect_by( const Cube<DIM> &fill ) const;

	void toStream( BinaryStream &stream ) const;
	std::string to_string() const;

private:
	void set_dimension( int dim, const Interval &i );
	std::array<Interval,DIM> dims;
};

class Point2 : public Point<2> {
	Point2() {};
	Point2( double x, double y ) {
		set_value(0,x);
		set_value(1,y);
	}
};

class Point3: public Point<3> {
	Point3() {};
	Point3( double x, double y, double z ) {
		set_value(0,x);
		set_value(1,y);
		set_value(2,z);
	}
};


class Cube2 : public Cube<2> {
public:
	Cube2() {};
	Cube2( BinaryStream &stream ) : Cube<2>(stream) {};
	Cube2( double x1, double x2, double y1, double y2 ) {
		set_dimension( 0, x1, x2 );
		set_dimension( 1, y1, y2 );
	};
};

class Cube3 : public Cube<3> {
public:
	Cube3() {};
	Cube3( BinaryStream &stream ) : Cube<3>(stream) {};
	Cube3( double x1, double x2, double y1, double y2, double z1, double z2 ) {
		set_dimension( 0, x1, x2 );
		set_dimension( 1, y1, y2 );
		set_dimension( 2, z1, z2 );
	};
};

#endif /* CUBE_H_ */
