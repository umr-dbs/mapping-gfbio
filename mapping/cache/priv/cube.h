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


/**
 * Models a closed interval [a,b]
 */
class Interval {
public:
	/**
	 * Constructs a new empty interval
	 */
	Interval();
	/**
	 * Constructs a new interval
	 * @param a the lower bound (inclusive)
	 * @param b the upper boud (inclusive)
	 */
	Interval( double a, double b );


	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	Interval( BinaryReadBuffer &buffer );

	/**
	 * @return whether this interval is empty (a == b)
	 */
	bool empty() const;

	/**
	 * @param other the interval to check for intersection
	 * @return whether this inteval intersects the given one
	 */
	bool intersects( const Interval &other ) const;

	/**
	 * @param other the interval to check for containment
	 * @return whether this interval contains the given one
	 */
	bool contains( const Interval &other ) const;

	/**
	 * @param value the value to check for containment
	 * @return whether this interval contains the given value
	 */
	bool contains( double value ) const;

	/**
	 * @param o the interval to check for equality
	 * @return whether this interval is equal to the given one
	 */
	bool operator==( const Interval &o ) const;

	/**
	 * @param o the interval to check for inequality
	 * @return whether this interval is NOT equal to the given one
	 */
	bool operator!=( const Interval &o ) const;

	/**
	 * @param other the interval to combine with
	 * @return a new interval containing this and the given interval
	 */
	Interval combine( const Interval &other ) const;

	/**
	 * @param other the interval to intersect
	 * @return a new interval which is the intersection of this and the given interval
	 */
	Interval intersect( const Interval & other ) const;

	/**
	 * @return the distance covered by this interval
	 */
	double distance() const;

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &stream, bool is_persistent_memory) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	double a, b;
};

/**
 * Models a DIM-dimensional point
 */
template <int DIM>
class Point {
	friend class Cube<DIM>;
public:
	/**
	 * Constructs a new instance
	 */
	Point();

	/**
	 * Gets the coordinate value for the given dimension
	 * @param dim the dimension
	 * @return the coordinate value for the given dimension
	 */
	double get_value( int dim ) const;

	/**
	 * Sets the coordinate value for the given dimension
	 * @param dim the dimension
	 * @param value the value to set
	 */
	void set_value( int dim, double value );

	/**
	 * @param o the point to check for equality
	 * @return whether this point is equal to the given one
	 */
	bool operator==( const Point<DIM> &o ) const;

	/**
	 * @param o the point to check for inequality
	 * @return whether this point is NOT equal to the given one
	 */
	bool operator!=( const Point<DIM> &o ) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;
private:
	std::array<double,DIM> values;
};

/**
 * Models a DIM-dimensional Hypercube by using intervals
 */
template <int DIM>
class Cube {
public:
	/**
	 * Constructs a new instance
	 */
	Cube();

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	Cube( BinaryReadBuffer &buffer );


	/**
	 * Gets the cubes extension for the given dimension
	 * @param dim the dimension to retrieve the interval for
	 * @return the interval for the given dimension
	 */
	const Interval& get_dimension( int dim ) const;

	/**
	 * Sets the cubes extend for the given dimension
	 * @param dim the dimensom to modify
	 * @param a the lower bound
	 * @param b the upper bound
	 */
	void set_dimension( int dim, double a, double b );

	/**
	 * @return whether this cube is empty (all intervals are empty)
	 */
	bool empty() const;

	/**
	 * @param other the cube to check for intersection
	 * @return whether this cube intersects the given one
	 */
	bool intersects( const Cube<DIM> &other ) const;

	/**
	 * @param other the cube to check for containment
	 * @return whether this cube contains the given one
	 */
	bool contains( const Cube<DIM> &other ) const;

	/**
	 * @param p the point to check for containment
	 * @return whether this cube contains the given point
	 */
	bool contains( const Point<DIM> &p ) const;

	/**
	 * @param o the cube to check for equality
	 * @return whether this cube is equal to the given one
	 */
	bool operator==( const Cube<DIM> &o ) const;

	/**
	 * @param o the cube to check for inequality
	 * @return whether this cube is NOT equal to the given one
	 */
	bool operator!=( const Cube<DIM> &o ) const;

	/**
	 * @return the volume of this cube
	 */
	double volume() const;

	/**
	 * @param other the cube to combine with
	 * @return a new cube containing this and the given cube
	 */
	Cube<DIM> combine( const Cube<DIM> &other ) const;

	/**
	 * @param other the cube to intersect
	 * @return a new cube which is the intersection of this and the given cube
	 */
	Cube<DIM> intersect( const Cube<DIM> &other ) const;

	/**
	 * @return the center of this cube
	 */
	Point<DIM> get_centre_of_mass() const;

	/**
	 * Dissects this cube by using the given one and
	 * returns the resulting remainder-cubes (max. 2*DIM).
	 * @param fill the cube which is used to dissect this one
	 * @return a list of remainder-cubes covering the areas not covered by fill
	 */
	std::vector<Cube<DIM>> dissect_by( const Cube<DIM> &fill ) const;

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

private:
	/**
	 * Sets the cubes extend for the given dimension
	 * @param dim the dimensom to modify
	 * @param i the new extend
	 */
	void set_dimension( int dim, const Interval &i );
	std::array<Interval,DIM> dims;
};

/**
 * Models a 2-dimensional point
 */
class Point2 : public Point<2> {
	Point2() {};
	Point2( double x, double y ) {
		set_value(0,x);
		set_value(1,y);
	}
};

/**
 * Models a 3-dimensional point
 */
class Point3: public Point<3> {
	Point3() {};
	Point3( double x, double y, double z ) {
		set_value(0,x);
		set_value(1,y);
		set_value(2,z);
	}
};

/**
 * Models a 2-dimensional cube
 */
class Cube2 : public Cube<2> {
public:
	Cube2() {};
	Cube2( BinaryReadBuffer &buffer ) : Cube<2>(buffer) {};
	Cube2( double x1, double x2, double y1, double y2 ) {
		set_dimension( 0, x1, x2 );
		set_dimension( 1, y1, y2 );
	};
};

/**
 * Models a 3-dimensional cube
 */
class Cube3 : public Cube<3> {
public:
	Cube3() {};
	Cube3( BinaryReadBuffer &buffer ) : Cube<3>(buffer) {};
	Cube3( double x1, double x2, double y1, double y2, double z1, double z2 ) {
		set_dimension( 0, x1, x2 );
		set_dimension( 1, y1, y2 );
		set_dimension( 2, z1, z2 );
	};
};

#endif /* CUBE_H_ */
