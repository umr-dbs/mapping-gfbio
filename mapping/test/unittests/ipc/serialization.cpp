
class BinaryReadBuffer;
static void compareBinaryReadBuffers(const BinaryReadBuffer &a, const BinaryReadBuffer &b);
#include "util/binarystream.h"
#include "util/make_unique.h"

#include "datatypes/raster.h"
#include "datatypes/attributes.h"

#include <gtest/gtest.h>

#include <unistd.h>
#include <fcntl.h>


template<typename T>
std::unique_ptr<BinaryReadBuffer> getSerializedBuffer(T &&obj) {
	int fds[2];
	int status = pipe2(fds, O_NONBLOCK | O_CLOEXEC);
	EXPECT_EQ(0, status);

	BinaryFDStream stream(fds[0], fds[1]);
	BinaryWriteBuffer wb;
	wb.write(obj, true);
	stream.write(wb);

	auto rb = make_unique<BinaryReadBuffer>();
	stream.read(*rb);

	return rb;
}

static void compareBinaryReadBuffers(const BinaryReadBuffer &a, const BinaryReadBuffer &b) {
	EXPECT_EQ(a.getPayloadSize(), b.getPayloadSize());
	EXPECT_EQ(a.buffer.size(), b.buffer.size());
	for (size_t i = 0;i<a.buffer.size();i++)
		EXPECT_EQ(a.buffer[i], b.buffer[i]);
}

TEST(Serialization, SpatioTemporalReference) {
	SpatioTemporalReference ref1(
		SpatialReference(EPSG_LATLON, -180, -90, 180, 90),
		TemporalReference(TIMETYPE_UNIX, 0, 1)
	);
	auto buf1 = getSerializedBuffer(ref1);

	SpatioTemporalReference ref2(*buf1);
	auto buf2 = getSerializedBuffer(ref2);

	compareBinaryReadBuffers(*buf1, *buf2);
}

TEST(Serialization, Raster) {
	DataDescription dd(GDT_Byte, Unit::unknown());
	SpatioTemporalReference stref(SpatialReference::unreferenced(), TemporalReference::unreferenced());
	auto raster1 = GenericRaster::create(dd, stref, 200, 20, 1, GenericRaster::Representation::CPU);
	raster1->clear(0);
	raster1->printCentered(2, "Test-string on a raster");
	auto buf1 = getSerializedBuffer(*raster1);

	auto raster2 = GenericRaster::deserialize(*buf1);
	auto buf2 = getSerializedBuffer(*raster2);

	compareBinaryReadBuffers(*buf1, *buf2);
}

TEST(Serialization, AttributeMaps) {
	AttributeMaps attr1;
	attr1.setTextual("question", "6*7");
	attr1.setNumeric("answer", 42);
	attr1.setTextual("key", "value");
	attr1.setNumeric("keycount", 4);
	auto buf1 = getSerializedBuffer(attr1);

	AttributeMaps attr2(*buf1);
	auto buf2 = getSerializedBuffer(attr2);

	compareBinaryReadBuffers(*buf1, *buf2);
}

TEST(Serialization, AttributeArrays) {
	const size_t count = 100;
	AttributeArrays attr1;
	auto &tex = attr1.addTextualAttribute("name", Unit::unknown());
	for (size_t i=0;i<count;i++)
		tex.set(i, "test");
	auto &num = attr1.addNumericAttribute("value", Unit::unknown());
	for (size_t i=0;i<count;i++)
		num.set(i, i+1);
	attr1.validate(count);
	auto buf1 = getSerializedBuffer(attr1);
	AttributeArrays attr2(*buf1);
	auto buf2 = getSerializedBuffer(attr2);

	compareBinaryReadBuffers(*buf1, *buf2);
}
