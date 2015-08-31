/*
 * trace.h
 *
 *  Created on: 16.06.2015
 *      Author: mika
 */

#ifndef PUZZLETRACER_H_
#define PUZZLETRACER_H_

#include "datatypes/raster.h"

#include <atomic>
#include <string>
#include <sys/stat.h>
#include <iostream>
#include <fstream>
#include <geos/geom/Geometry.h>

//
// Helper to trace the process of puzzling raster-requests
//

class RasterWriter {
public:
	RasterWriter( std::string dir );
	RasterWriter( RasterWriter &&rw );
	~RasterWriter();
	void write_raster( GenericRaster &raster, std::string prefix = "" );
	void write_meta( const QueryRectangle &query, const geos::geom::Geometry &covered );
	const std::string dir;
private:
	int file_no;
	std::ofstream meta;
};

class PuzzleTracer {
public:
	static void init();
	static RasterWriter get_writer();
private:
	static std::atomic_int next;
	static std::string dir;
};

#endif /* PUZZLETRACER_H_ */
