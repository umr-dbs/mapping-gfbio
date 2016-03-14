
#include "datatypes/raster/raster_priv.h"
#include "datatypes/raster/typejuggling.h"
#ifndef MAPPING_NO_OPENCL
#include "raster/opencl.h"
#endif
#include "raster/profiler.h"
#include "util/hash.h"
#include "util/binarystream.h"
#include "operators/operator.h" // for QueryRectangle

#include <memory>
#include <cmath>
#include <limits>
#include <vector>
#include <string>
#include <sstream>


void DataDescription::verify() const {
	if (has_no_data) {
		if (datatype == GDT_Float32 && std::isnan(no_data)) {
			// valid
		}
		else if (!std::isfinite(no_data)) {
			throw MetadataException("ValueMetadata::verify: no_data neither finite nor NaN");
		}
		else if (no_data < getMinByDatatype() || no_data > getMaxByDatatype()) {
			throw MetadataException("ValueMetadata::verify: no_data outside of range allowed by datatype");
		}
	}
}

int DataDescription::getBPP() const {
	switch(datatype) {
		case GDT_Byte: return sizeof(uint8_t);
		case GDT_Int16: return sizeof(int16_t);
		case GDT_UInt16: return sizeof(uint16_t);
		case GDT_Int32: return sizeof(int32_t);
		case GDT_UInt32: return sizeof(uint32_t);
		case GDT_Float32: return sizeof(float);
		case GDT_Float64: return sizeof(double);
		case GDT_CInt16:
			throw MetadataException("Unsupported data type: CInt16");
		case GDT_CInt32:
			throw MetadataException("Unsupported data type: CInt32");
		case GDT_CFloat32:
			throw MetadataException("Unsupported data type: CFloat32");
		case GDT_CFloat64:
			throw MetadataException("Unsupported data type: CFloat64");
		default:
			throw MetadataException("Unknown data type");
	}
}

double DataDescription::getMinByDatatype() const {
	switch(datatype) {
		case GDT_Byte: return std::numeric_limits<uint8_t>::min();
		case GDT_Int16: return std::numeric_limits<int16_t>::min();
		case GDT_UInt16: return std::numeric_limits<uint16_t>::min();
		case GDT_Int32: return std::numeric_limits<int32_t>::min();
		case GDT_UInt32: return std::numeric_limits<uint32_t>::min();
		case GDT_Float32: return std::numeric_limits<float>::lowest();
		case GDT_Float64: return std::numeric_limits<double>::lowest();
		case GDT_CInt16:
			throw MetadataException("Unsupported data type: CInt16");
		case GDT_CInt32:
			throw MetadataException("Unsupported data type: CInt32");
		case GDT_CFloat32:
			throw MetadataException("Unsupported data type: CFloat32");
		case GDT_CFloat64:
			throw MetadataException("Unsupported data type: CFloat64");
		default:
			throw MetadataException("Unknown data type");
	}
}

double DataDescription::getMaxByDatatype() const {
	switch(datatype) {
		case GDT_Byte: return std::numeric_limits<uint8_t>::max();
		case GDT_Int16: return std::numeric_limits<int16_t>::max();
		case GDT_UInt16: return std::numeric_limits<uint16_t>::max();
		case GDT_Int32: return std::numeric_limits<int32_t>::max();
		case GDT_UInt32: return std::numeric_limits<uint32_t>::max();
		case GDT_Float32: return std::numeric_limits<float>::max();
		case GDT_Float64: return std::numeric_limits<double>::max();
		case GDT_CInt16:
			throw MetadataException("Unsupported data type: CInt16");
		case GDT_CInt32:
			throw MetadataException("Unsupported data type: CInt32");
		case GDT_CFloat32:
			throw MetadataException("Unsupported data type: CFloat32");
		case GDT_CFloat64:
			throw MetadataException("Unsupported data type: CFloat64");
		default:
			throw MetadataException("Unknown data type");
	}
}


std::ostream& operator<< (std::ostream &out, const DataDescription &dd) {
	out << "Datatype: " << dd.datatype;
	if (dd.has_no_data)
		out << " nodata = " << dd.no_data;
	else
		out << " no nodata";
	out << std::endl;
	return out;
}

void DataDescription::addNoData() {
	if (has_no_data)
		return;

	if (datatype == GDT_Float32 || datatype == GDT_Float64) {
		no_data = std::numeric_limits<double>::quiet_NaN();
		has_no_data = true;
		return;
	}

	double real_min = getMinByDatatype();
	double real_max = getMaxByDatatype();
	if (real_min <= unit.getMin() - 1) {
		no_data = unit.getMin() - 1;
	}
	else if (real_max >= unit.getMax() + 1) {
		no_data = unit.getMax() + 1;
	}
	else
		throw MetadataException("Cannot add value for no_data: range of datatype is exhausted.");

	has_no_data = true;
}

void DataDescription::serialize(BinaryWriteBuffer &buffer) const {
	buffer.write(datatype);
	buffer.write(unit.toJson());
	buffer.write(has_no_data);
	if (has_no_data)
		buffer.write(no_data);
}
DataDescription::DataDescription(BinaryReadBuffer &buffer) : unit{Unit::UNINITIALIZED} {
	buffer.read(&datatype);
	auto unitstr = buffer.read<std::string>();
	unit = Unit(unitstr);
	buffer.read(&has_no_data);
	if (has_no_data)
		buffer.read(&no_data);
	else
		no_data = 0.0;
}

size_t DataDescription::get_byte_size() const {
	return sizeof(GDALDataType) + sizeof(double) + sizeof(bool) + unit.get_byte_size();
}



std::unique_ptr<GenericRaster> GenericRaster::create(const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth, Representation representation) {
	GenericRaster *result = nullptr;
	switch(datadescription.datatype) {
		case GDT_Byte:
			result = new Raster2D<uint8_t>(datadescription, stref, width, height);
			break;
		case GDT_Int16:
			result = new Raster2D<int16_t>(datadescription, stref, width, height);
			break;
		case GDT_UInt16:
			result = new Raster2D<uint16_t>(datadescription, stref, width, height);
			break;
		case GDT_Int32:
			result = new Raster2D<int32_t>(datadescription, stref, width, height);
			break;
		case GDT_UInt32:
			result = new Raster2D<uint32_t>(datadescription, stref, width, height);
			break;
		case GDT_Float32:
			result = new Raster2D<float>(datadescription, stref, width, height);
			break;
		case GDT_Float64:
			result = new Raster2D<double>(datadescription, stref, width, height);
			break;
		case GDT_CInt16:
			throw MetadataException("Unsupported data type: CInt16");
		case GDT_CInt32:
			throw MetadataException("Unsupported data type: CInt32");
		case GDT_CFloat32:
			throw MetadataException("Unsupported data type: CFloat32");
		case GDT_CFloat64:
			throw MetadataException("Unsupported data type: CFloat64");
		default:
			throw MetadataException("Unknown data type");
	}

	result->setRepresentation(representation);
	return std::unique_ptr<GenericRaster>(result);
}

GenericRaster::GenericRaster(const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth)
	: GridSpatioTemporalResult(stref, width, height), dd(datadescription), representation(Representation::CPU) {

	if (depth != 0 || width == 0 || height == 0)
		throw MetadataException("Cannot instantiate raster with dimensions != 2 yet");

	double stwidth = std::abs(stref.x2 - stref.x1);
	double stheight = std::abs(stref.y2 - stref.y1);
	if (!std::isfinite(stwidth) || !std::isfinite(stheight))
		throw MetadataException("Cannot instantiate raster on SpatioTemporalReference with infinite size");
}

GenericRaster::~GenericRaster() {
}

void GenericRaster::serialize(BinaryWriteBuffer &buffer) {
	const char *data = (const char *) getData();
	size_t len = getDataSize();
	buffer.write(dd);
	buffer.write(stref);
	buffer.write((uint32_t) width);
	buffer.write((uint32_t) height);
	buffer.write(data, len, true);
	buffer.write(global_attributes);
}

std::unique_ptr<GenericRaster> GenericRaster::deserialize(BinaryReadBuffer &buffer) {
	DataDescription dd(buffer);
	SpatioTemporalReference stref(buffer);
	uint32_t width, height;
	buffer.read(&width);
	buffer.read(&height);

	auto raster = GenericRaster::create(dd, stref, width, height);
	char *data = (char *) raster->getDataForWriting();
	size_t len = raster->getDataSize();
	buffer.read(data, len);
	raster->global_attributes.deserialize(buffer);

	return raster;
}


std::unique_ptr<GenericRaster> GenericRaster::clone() {
	setRepresentation(GenericRaster::Representation::CPU);

	auto copy = GenericRaster::create(dd, *this, GenericRaster::Representation::CPU);
	copy->global_attributes = global_attributes;
	memcpy(copy->getDataForWriting(), getData(), getDataSize() );

	return copy;
}



static void * alloc_aligned_buffer(size_t size) {
	const size_t ALIGN = 4096;

	if (size % ALIGN != 0)
		size = size - (size % ALIGN) + ALIGN;

	void *data = aligned_alloc(ALIGN, size);
	if (data == nullptr)
		throw std::bad_alloc();
	memset(data, 0, size);
	return data;
}


template<typename T, int dimensions>
Raster<T, dimensions>::Raster(const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth)
	: GenericRaster(datadescription, stref, width, height, depth), clhostptr(nullptr),clbuffer(nullptr), clbuffer_info(nullptr) {
	auto count = getPixelCount();

	size_t required_size = (count+1) * sizeof(T);
	data = (T *) alloc_aligned_buffer(required_size);
	//data = new T[count + 1];

	data[count] = 42;
}

#define MAPPING_OPENCL_USE_HOST_PTR 1

template<typename T, int dimensions>
Raster<T, dimensions>::~Raster() {
	// TODO: is a raster that's currently on the GPU correctly freed?
	//setRepresentation(GenericRaster::Representation::CPU);

#ifndef MAPPING_NO_OPENCL
#if MAPPING_OPENCL_USE_HOST_PTR
	if ( clhostptr ) {
		RasterOpenCL::getQueue()->enqueueUnmapMemObject(*clbuffer, clhostptr);
		clhostptr = nullptr;
	}
#endif

	if (clbuffer) {
		delete clbuffer;
		clbuffer = nullptr;
	}
	if (clbuffer_info) {
		delete clbuffer_info;
		clbuffer_info = nullptr;
	}
#endif

	if (data) {
		if (data[getPixelCount()] != 42) {
			printf("Error in Raster: guard value was overwritten. Memory corruption!\n");
			exit(6);
		}

		free(data);
		//delete [] data;
		data = nullptr;
	}
}


template<typename T>
Raster2D<T>::Raster2D(const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth )
	:Raster<T, 2>(datadescription, stref, width, height, depth) {
}


template<typename T>
Raster2D<T>::~Raster2D() {
}

template<typename T, int dimensions>
void Raster<T, dimensions>::setRepresentation(Representation r) {
	if (r == representation)
		return;
	if (r == Representation::OPENCL) {
#ifdef MAPPING_NO_OPENCL
		throw PlatformException("No OpenCL support");
#else
		// https://www.khronos.org/registry/cl/sdk/1.0/docs/man/xhtml/clCreateBuffer.html
		try {
			//Profiler::Profiler p("migrate to GPU");
#if MAPPING_OPENCL_USE_HOST_PTR
			clbuffer = new cl::Buffer(
				*RasterOpenCL::getContext(),
				CL_MEM_USE_HOST_PTR,
				getDataSize(),
				data
			);
			clhostptr = RasterOpenCL::getQueue()->enqueueMapBuffer(*clbuffer, CL_TRUE, CL_MAP_READ | CL_MAP_WRITE, 0, getDataSize());
#else
			//printf("Creating clbuffer with size %lu of %lu\n", getDataSize(), RasterOpenCL::getMaxAllocSize());
			clbuffer = new cl::Buffer(
				*RasterOpenCL::getContext(),
				CL_MEM_READ_WRITE, // | CL_MEM_USE_HOST_PTR, // CL_MEM_COPY_HOST_PTR
				getDataSize(),
				nullptr //data
			);
			RasterOpenCL::getQueue()->enqueueWriteBuffer(*clbuffer, CL_TRUE, 0, getDataSize(), data);
			//delete [] data;
			free(data);
			data = nullptr;
#endif
		}
		catch (cl::Error &e) {
			std::stringstream ss;
			ss << "CL Error in Raster::setRepresentation(): " << e.err() << ": " << e.what();
			throw OpenCLException(ss.str());
		}

		clbuffer_info = RasterOpenCL::getBufferWithRasterinfo(this).release();
#endif
	}
	else if (r == Representation::CPU) {
#ifdef MAPPING_NO_OPENCL
		throw PlatformException("No OpenCL support");
#else
		{
			//Profiler::Profiler p("migrate to CPU");
#if MAPPING_OPENCL_USE_HOST_PTR
			RasterOpenCL::getQueue()->enqueueUnmapMemObject(*clbuffer, clhostptr);
			clhostptr = nullptr;
#else
			auto count = getPixelCount();
			size_t required_size = (count+1) * sizeof(T);
			data = (T *) alloc_aligned_buffer(required_size);
			//data = new T[count + 1];
			data[count] = 42;

			RasterOpenCL::getQueue()->enqueueReadBuffer(*clbuffer, CL_TRUE, 0, getDataSize(), data);
#endif
		}
		if (clbuffer) {
			delete clbuffer;
			clbuffer = nullptr;
		}
		if (clbuffer_info) {
			delete clbuffer_info;
			clbuffer_info = nullptr;
		}
#endif
	}
	else
		throw MetadataException("Invalid representation chosen");

	representation = r;
}



template<typename T>
void Raster2D<T>::clear(double _value) {
	T value = (T) _value;

	setRepresentation(GenericRaster::Representation::CPU);
	auto size = getPixelCount();
	for (decltype(size) i=0;i<size;i++) {
		data[i] = value;
	}
}


template<typename T>
void Raster2D<T>::blit(const GenericRaster *genericraster, int destx, int desty, int) {
	if (genericraster->dd.datatype != dd.datatype)
		throw MetadataException("blit with incompatible raster");

	if (genericraster->stref.epsg != stref.epsg && stref.epsg != EPSG_UNREFERENCED && genericraster->stref.epsg != EPSG_UNREFERENCED)
		throw MetadataException("blit of raster with different coordinate system");

	setRepresentation(GenericRaster::Representation::CPU);
	if (genericraster->getRepresentation() != GenericRaster::Representation::CPU)
		throw MetadataException("blit from raster that's not in a CPU buffer");

	Raster2D<T> *raster = (Raster2D<T> *) genericraster;
	int x1 = std::max(destx, 0);
	int y1 = std::max(desty, 0);
	int x2 = std::min(width, destx+raster->width);
	int y2 = std::min(height, desty+raster->height);

/*
	fprintf(stderr, "this raster is %dx%d\n", lcrs.size[0], lcrs.size[1]);
	fprintf(stderr, "other raster is %dx%d\n", raster->lcrs.size[0], raster->lcrs.size[1]);
	fprintf(stderr, "blitting other at (%d,%d) to (%d,%d) -> (%d,%d)\n", destx, desty, x1, y1, x2, y2);
*/
	if (x1 >= x2 || y1 >= y2)
		throw MetadataException("blit without overlapping region");

#define BLIT_TYPE 2
#if BLIT_TYPE == 1 // 0.0286
	for (int y=y1;y<y2;y++)
		for (int x=x1;x<x2;x++) {
			set(x, y, raster->get(x-destx, y-desty));
		}
#elif BLIT_TYPE == 2 // 0.0246
	int blitwidth = x2-x1;
	for (int y=y1;y<y2;y++) {
		int rowoffset_dest = y * this->width + x1;
		int rowoffset_src = (y-desty) * raster->width + (x1-destx);
		memcpy(&data[rowoffset_dest], &raster->data[rowoffset_src], blitwidth * sizeof(T));
	}
#else // 0.031
	int blitwidth = x2-x1;
	for (int y=y1;y<y2;y++) {
		int rowoffset_dest = y * this->width + x1;
		int rowoffset_src = (y-desty) * raster->width + (x1-destx);
		for (int x=0;x<blitwidth;x++)
			data[rowoffset_dest+x] = raster->data[rowoffset_src+x];
	}
#endif
}


template<typename T>
std::unique_ptr<GenericRaster> Raster2D<T>::cut(int x1, int y1, int z1, int width, int height, int depth) {
	if (z1 != 0 || depth != 0)
		throw MetadataException("cut() should not specify 3d coordinates on a 2d raster");

	if (x1 < 0 || x1 + width > (int) this->width || y1 < 0 || y1 + height > (int) this->height)
		throw MetadataException("cut() not inside the raster");

	double world_x1 = PixelToWorldX(x1) - pixel_scale_x * 0.5;
	double world_y1 = PixelToWorldY(y1) - pixel_scale_y * 0.5;
	double world_x2 = world_x1 + pixel_scale_x * width;
	double world_y2 = world_y1 + pixel_scale_y * height;
	SpatioTemporalReference newstref(
		SpatialReference(stref.epsg, world_x1, world_y1, world_x2, world_y2),
		TemporalReference(stref)
	);

	auto outputraster_guard = GenericRaster::create(dd, newstref, width, height);
	Raster2D<T> *outputraster = (Raster2D<T> *) outputraster_guard.get();

#define CUT_TYPE 2
#if CUT_TYPE == 1 // 0.0286
	for (int y=0;y<height;y++)
		for (int x=0;x<width;x++)
			outputraster->set(x, y, getSafe(x+x1, y+y1));
#elif CUT_TYPE == 2 // 0.0246
	for (int y=0;y<height;y++) {
		size_t rowoffset_src = (size_t) (y+y1) * width + x1;
		size_t rowoffset_dest = (size_t) y * width;
		memcpy(&outputraster->data[rowoffset_dest], &data[rowoffset_src], width * sizeof(T));
	}
/*
#else // 0.031
	int blitwidth = x2-x1;
	for (int y=y1;y<y2;y++) {
		int rowoffset_dest = y * this->width + x1;
		int rowoffset_src = y * raster->width + x1;
		for (int x=0;x<blitwidth;x++)
			data[rowoffset_dest+x] = raster->data[rowoffset_src+x];
	}
*/
#endif

	outputraster_guard->global_attributes = this->global_attributes;
	return outputraster_guard;
}

template<typename T>
std::unique_ptr<GenericRaster> Raster2D<T>::scale(int width, int height, int depth) {
	if (depth != 0)
		throw MetadataException("scale() should not specify z depth on a 2d raster");

	if (width <= 0 || height <= 0)
		throw MetadataException("scale() to empty area not allowed");

	this->setRepresentation(GenericRaster::CPU);


	auto outputraster_guard = GenericRaster::create(dd, stref, width, height, depth);
	Raster2D<T> *outputraster = (Raster2D<T> *) outputraster_guard.get();
	//outputraster->clear(dd.no_data);

	int64_t src_width = this->width, src_height = this->height;

	for (int y=0;y<height;y++) {
		for (int x=0;x<width;x++) {
			int px = (int) round( ((x+0.5) * src_width / width) - 0.5 );
			int py = (int) round( ((y+0.5) * src_height / height) - 0.5 );

			/*
			if (px < 0 || py < 0 || px >= src_width || py >= src_height) {
				fprintf(stderr, "point outside: size (%ld, %ld) -> (%d, %d), point (%d, %d) -> (%d, %d)\n",
					src_width, src_height, width, height,
					x, y, px, py
				);
				continue;
			}
			*/

			outputraster->set(x, y, get(px, py));
		}
	}

	outputraster_guard->global_attributes = this->global_attributes;
	return outputraster_guard;
}

template<typename T>
std::unique_ptr<GenericRaster> Raster2D<T>::flip(bool flipx, bool flipy) {
	auto flipped_raster = GenericRaster::create(dd, *this);
	Raster2D<T> *r = (Raster2D<T> *) flipped_raster.get();

	setRepresentation(GenericRaster::Representation::CPU);

	for (uint32_t y=0;y<height;y++) {
		uint32_t py = flipy ? height-y-1 : y;
		for (uint32_t x=0;x<width;x++) {
			uint32_t px = flipx ? width-x-1 : x;
			r->set(x, y, get(px, py));
		}
	}

	flipped_raster->global_attributes = this->global_attributes;
	return flipped_raster;
}


/*
 * This class is a performance optimization to reproject between two rasters of the same CRS.
 *
 * The basic formula is this:
 * source_x = source->WorldToPixelX( dest->PixelToWorldX( dest_x ) );
 *
 * But that involves several mathematical operations we can precalculate.
 */
class GridSpatioTemporalResultProjecter {
public:
	GridSpatioTemporalResultProjecter(const GridSpatioTemporalResult &source, const GridSpatioTemporalResult &dest) {
		if (source.stref.epsg != dest.stref.epsg)
			throw ArgumentException("Cannot do simple projections between rasters of a different epsg");
		// source_x = WorldToPixelX( PixelToWorldX( dest_x ) );
		// source_x = WorldToPixelY( dest.stref.x1 + (dest_x+0.5) * dest.pixel_scale_x )
		// source_x = floor( ( (dest.stref.x1 + (dest_x+0.5) * dest.pixel_scale_x) - source.stref.x1) / source.pixel_scale_x )
		// source_x = floor( ( (dest.stref.x1 + (dest_x+0.5) * dest.pixel_scale_x) - source.stref.x1) / source.pixel_scale_x )
		// source_x = floor( dest_x * dest.pixel_scale_x/source.pixel_scale_x + (dest.stref.x1 + 0.5*dest.pixel_scale_x - source.stref.x1) / source.pixel_scale_x )
		factor_x = dest.pixel_scale_x/source.pixel_scale_x;
		add_x = (dest.stref.x1 + 0.5*dest.pixel_scale_x - source.stref.x1) / source.pixel_scale_x;

		factor_y = dest.pixel_scale_y/source.pixel_scale_y;
		add_y = (dest.stref.y1 + 0.5*dest.pixel_scale_y - source.stref.y1) / source.pixel_scale_y;
	}
	int64_t getX(int px) { return floor(px * factor_x + add_x); }
	int64_t getY(int py) { return floor(py * factor_y + add_y); }
private:
	double factor_x, factor_y;
	double add_x, add_y;
};


template<typename T>
std::unique_ptr<GenericRaster> Raster2D<T>::fitToQueryRectangle(const QueryRectangle &qrect) {
	setRepresentation(GenericRaster::Representation::CPU);

	// adjust sref and resolution, but keep the tref.
	QueryRectangle target(qrect, stref, qrect);

	auto out = GenericRaster::create(dd, target, target.xres, target.yres);
	Raster2D<T> *r = (Raster2D<T> *) out.get();

	GridSpatioTemporalResultProjecter p(*this, *out);
	for (uint32_t y=0;y<r->height;y++) {
		//auto py = this->WorldToPixelY( r->PixelToWorldY(y) );
		auto py = p.getY(y);
		for (uint32_t x=0;x<r->width;x++) {
			//auto px = this->WorldToPixelX( r->PixelToWorldX(x) );
			auto px = p.getX(x);
			r->set(x, y, getSafe(px, py));
		}
	}

	out->global_attributes = this->global_attributes;
	return out;
}

template<typename T>
double Raster2D<T>::getAsDouble(int x, int y, int) const {
	return (double) get(x, y);
}



std::string GenericRaster::hash() {
	setRepresentation(GenericRaster::Representation::CPU);
	const int len = getDataSize();
	const uint8_t * data = (const uint8_t*) getData();

	return calculateHash(data, len).asHex();
}


#include "raster_font.h"
template<typename T>
void Raster2D<T>::print(int dest_x, int dest_y, double dvalue, const char *text, int maxlen) {
	if (maxlen < 0)
		maxlen = strlen(text);

	T value = (T) dvalue;

	this->setRepresentation(GenericRaster::CPU);

	for (;maxlen > 0 && *text;text++, maxlen--) {
		int src_x = (*text % 16) * 8;
		int src_y = (*text / 16) * 8;
		for (int y=0;y<8;y++) {
			for (int x=0;x<8;x++) {
				int font_pixel = (x + src_x) + (y + src_y) * 128;
				int font_byte = font_pixel / 8;
				int font_bit = font_pixel % 8;

				int f = raster_font_bits[font_byte] & (1 << font_bit);
				if (f)
					this->setSafe(x+dest_x, y+dest_y, value);
			}
		}
		dest_x += 8;
	}
}


void GenericRaster::printCentered(double dvalue, const char *text) {
	const int BORDER = 16;

	int len = strlen(text);

	int width = this->width - 2*BORDER;
	int height = this->height - 2*BORDER;

	int max_chars_x = width / 8;
	int max_chars_y = height / 8;

	int lines_required = (len + max_chars_x - 1) / max_chars_x;
	int offset_y = (height - 8*lines_required) / 2;

	for (int line=0;line < max_chars_y && line*max_chars_x < len;line++) {
		print(BORDER, BORDER+offset_y+8*line, dvalue, &text[line*max_chars_x], max_chars_x);
	}
}

size_t GenericRaster::get_byte_size() const {
	return GridSpatioTemporalResult::get_byte_size() + sizeof(Representation) + dd.get_byte_size();
}

RASTER_PRIV_INSTANTIATE_ALL
