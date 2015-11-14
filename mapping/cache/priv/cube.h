/*
 * Cube.h
 *
 *  Created on: 12.11.2015
 *      Author: mika
 */

#ifndef CUBE_H_
#define CUBE_H_

#include <vector>
#include <string>

class Interval {
public:
	Interval();
	Interval( double a, double b );
	Interval( const Interval &i );
	Interval( BinaryStream &stream );
	bool empty() const;
	bool intersects( const Interval &other ) const;
	bool contains( const Interval &other ) const;

	Interval combine( const Interval &other ) const;
	double distance() const;

	void toStream( BinaryStream &stream ) const;
	std::string to_string() const;

	double a;
	double b;
};

template <int DIM>
class Cube {
public:
	Cube();
	Cube( const Cube<DIM> &c );
	Cube( BinaryStream &stream );
	const Interval& get_dimension( int dim ) const;
	void set_dimension( int dim, double a, double b );

	bool empty() const;
	bool intersects( const Cube<DIM> &other ) const;
	bool contains( const Cube<DIM> &other ) const;

	double volume() const;
	Cube<DIM> combine( const Cube<DIM> &other ) const;

	std::vector<Cube<DIM>> dissect_by( const Cube<DIM> &fill ) const;

	void toStream( BinaryStream &stream ) const;
	std::string to_string() const;

private:
	void set_dimension( int dim, const Interval &i );
	Interval dims[DIM];
};

#endif /* CUBE_H_ */
