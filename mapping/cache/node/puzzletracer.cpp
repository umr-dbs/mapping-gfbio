/*
 * trace.cpp
 *
 *  Created on: 16.06.2015
 *      Author: mika
 */

#include <cache/common.h>
#include "cache/node/puzzletracer.h"
#include <sstream>


RasterWriter::RasterWriter(std::string dir) : dir(dir), file_no(1), meta(dir + "/meta.txt") {
}

RasterWriter::~RasterWriter() {
	meta.close();
}

RasterWriter::RasterWriter(RasterWriter&& rw) : dir(std::move(dir)), file_no(rw.file_no), meta(dir + "/meta.txt", std::ios::app) {
}

void RasterWriter::write_meta(const QueryRectangle& query, const geos::geom::Geometry& covered) {
	meta << "Query: " << CacheCommon::qr_to_string(query) << std::endl;
	meta << "Covered: " << covered.toString() << std::endl;
}

void RasterWriter::write_raster(GenericRaster& raster, std::string prefix) {
	int id = file_no++;
	std::ostringstream ss;
	ss << dir << "/" << prefix << id << ".png";
	auto col = Colorizer::make("");

	meta << prefix << id << std::endl;
	meta << "x: [" << raster.stref.x1 << "," << raster.stref.x2 << "], ";
	meta << "y[" << raster.stref.y1 << "," << raster.stref.y2 << "], ";
	meta << "t[" << raster.stref.t1 << "," << raster.stref.t2 << "], ";
	meta << "size: "<< raster.width << "x" << raster.height << ", ";
	meta << "res: " << raster.pixel_scale_x << "x" << raster.pixel_scale_y;
	meta << std::endl;
	raster.toPNG( ss.str().c_str(), *col, false, true );
}

std::atomic_int PuzzleTracer::next(1);
std::string PuzzleTracer::dir = "/tmp/";

void PuzzleTracer::init() {
	time_t t = time(0);

	std::ostringstream ss;
	ss << "/tmp/" << t;
	dir = ss.str();
	if ( 0 != mkdir(dir.c_str(), S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) )
		throw std::runtime_error("Fucked up");
}

RasterWriter PuzzleTracer::get_writer() {
	int id = next++;
	std::ostringstream ss;
	ss << dir << "/" << id;
	std::string w_dir = ss.str();
	if ( 0 != mkdir(w_dir.c_str(), S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) )
		throw std::runtime_error("Fucked up");
	return RasterWriter(w_dir);
}
