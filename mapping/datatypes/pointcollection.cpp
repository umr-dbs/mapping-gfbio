#include "datatypes/pointcollection.h"
#include "util/binarystream.h"
#include "util/hash.h"
#include "util/make_unique.h"

#include <sstream>
#include <iomanip>
#include <iostream>


Point::Point(double x, double y) : x(x), y(y) {
}

Point::~Point() {
}

Point::Point(BinaryStream &stream) {
	stream.read(&x);
	stream.read(&y);
}
void Point::toStream(BinaryStream &stream) {
	stream.write(x);
	stream.write(y);
}



PointCollection::PointCollection(epsg_t epsg) : epsg(epsg), has_time(false) {

}
PointCollection::~PointCollection() {

}

template<typename T>
std::unique_ptr<PointCollection> filter(PointCollection *in, const std::vector<T> &keep) {
	size_t count = in->collection.size();
	if (keep.size() != count) {
		std::ostringstream msg;
		msg << "PointCollection::filter(): size of filter does not match (" << keep.size() << " != " << count << ")";
		throw ArgumentException(msg.str());
	}

	size_t kept_count = 0;
	for (size_t idx=0;idx<count;idx++) {
		if (keep[idx])
			kept_count++;
	}

	auto out = std::make_unique<PointCollection>(in->epsg);
	out->collection.reserve(kept_count);
	// copy global metadata
	out->global_md_string = in->global_md_string;
	out->global_md_value = in->global_md_value;

	// copy points
	for (size_t idx=0;idx<count;idx++) {
		if (keep[idx]) {
			Point &p = in->collection[idx];
			out->addPoint(p.x, p.y);
		}
	}

	// copy local MD
	for (auto &keyValue : in->local_md_string) {
		const auto &vec_in = in->local_md_string.getVector(keyValue.first);
		auto &vec_out = out->local_md_string.addVector(keyValue.first, kept_count);
		for (size_t idx=0;idx<count;idx++) {
			if (keep[idx])
				vec_out.push_back(vec_in[idx]);
		}
	}

	for (auto &keyValue : in->local_md_value) {
		const auto &vec_in = in->local_md_value.getVector(keyValue.first);
		auto &vec_out = out->local_md_value.addVector(keyValue.first, kept_count);
		for (size_t idx=0;idx<count;idx++) {
			if (keep[idx])
				vec_out.push_back(vec_in[idx]);
		}
	}
	// copy time array
	out->has_time = false;
	out->timestamps.clear();
	if (in->has_time) {
		out->has_time = true;
		out->timestamps.reserve(kept_count);
		for (size_t idx=0;idx<count;idx++) {
			if (keep[idx])
				out->timestamps.push_back(in->timestamps[idx]);
		}
	}

	return out;
}

std::unique_ptr<PointCollection> PointCollection::filter(const std::vector<bool> &keep) {
	return ::filter<bool>(this, keep);
}

std::unique_ptr<PointCollection> PointCollection::filter(const std::vector<char> &keep) {
	return ::filter<char>(this, keep);
}

PointCollection::PointCollection(BinaryStream &stream) : epsg(EPSG_UNKNOWN), has_time(false) {
	stream.read(&epsg);
	size_t count;
	stream.read(&count);
	collection.reserve(count);

	global_md_string.fromStream(stream);
	global_md_value.fromStream(stream);
	local_md_string.fromStream(stream);
	local_md_value.fromStream(stream);

	for (size_t i=0;i<count;i++) {
		collection.push_back( Point(stream) );
	}
	// TODO: serialize/unserialize time array
}

void PointCollection::toStream(BinaryStream &stream) {
	stream.write(epsg);
	size_t count = collection.size();
	stream.write(count);

	stream.write(global_md_string);
	stream.write(global_md_value);
	stream.write(local_md_string);
	stream.write(local_md_value);

	for (size_t i=0;i<count;i++) {
		collection[i].toStream(stream);
	}
	// TODO: serialize/unserialize time array
}


size_t PointCollection::addPoint(double x, double y) {
	collection.push_back(Point(x, y));
	return collection.size() - 1;
}


/**
 * Global Metadata
 */
const std::string &PointCollection::getGlobalMDString(const std::string &key) const {
	return global_md_string.get(key);
}

double PointCollection::getGlobalMDValue(const std::string &key) const {
	return global_md_value.get(key);
}

DirectMetadata<std::string>* PointCollection::getGlobalMDStringIterator() {
	return &global_md_string;
}

DirectMetadata<double>* PointCollection::getGlobalMDValueIterator() {
	return &global_md_value;
}

std::vector<std::string> PointCollection::getGlobalMDValueKeys() const {
	std::vector<std::string> keys;
	for (auto keyValue : global_md_value) {
		keys.push_back(keyValue.first);
	}
	return keys;
}

std::vector<std::string> PointCollection::getGlobalMDStringKeys() const {
	std::vector<std::string> keys;
	for (auto keyValue : global_md_string) {
		keys.push_back(keyValue.first);
	}
	return keys;
}

void PointCollection::setGlobalMDString(const std::string &key, const std::string &value) {
	global_md_string.set(key, value);
}

void PointCollection::setGlobalMDValue(const std::string &key, double value) {
	global_md_value.set(key, value);
}



/**
 * Export
 */
#if 0
// http://www.gdal.org/ogr_apitut.html
#include "ogrsf_frmts.h"

void gdal_init(); // implemented in raster/import_gdal.cpp

void PointCollection::toOGR(const char *driver = "ESRI Shapefile") {
	gdal_init();

    GDALDriver *poDriver = GetGDALDriverManager()->GetDriverByName(pszDriverName );
    if (poDriver == nullptr)
    	throw ExporterException("OGR driver not available");

    GDALDataset *poDS = poDriver->Create( "point_out.shp", 0, 0, 0, GDT_Unknown, NULL );
    if (poDS == nullptr) {
    	// TODO: free driver?
    	throw ExporterException("Dataset creation failed");
    }

    OGRLayer *poLayer = poDS->CreateLayer( "point_out", NULL, wkbPoint, NULL );
    if (poLayer == nullptr) {
    	// TODO: free driver and dataset?
    	throw ExporterException("Layer Creation failed");
    }

    // No attributes

    // Loop over all points
	for (const Point &p : collection) {
		double x = p.x, y = p.y;

		OGRFeature *poFeature = OGRFeature::CreateFeature(poLayer->GetLayerDefn());
		//poFeature->SetField( "Name", szName );
		OGRPoint pt;
		pt.setX(p.x);
		pt.setY(p.y);
		poFeature->SetGeometry(&pt);

        if (poLayer->CreateFeature( poFeature ) != OGRERR_NONE) {
        	// TODO: free stuf..
        	throw ExporterException("CreateFeature failed");
        }

        OGRFeature::DestroyFeature( poFeature );
	}
	GDALClose(poDS);
}
#endif


std::string PointCollection::toGeoJSON(bool displayMetadata) {
/*
	  { "type": "MultiPoint",
	    "coordinates": [ [100.0, 0.0], [101.0, 1.0] ]
	    }
*/
	std::ostringstream json;
	json << std::fixed; // std::setprecision(4);

	if(displayMetadata && (local_md_value.size() > 0 || local_md_string.size() > 0 || has_time)) {

		json << "{\"type\":\"FeatureCollection\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:" << epsg <<"\"}},\"features\":[";

		size_t idx = 0;
		auto value_keys = local_md_value.getKeys();
		auto string_keys = local_md_string.getKeys();
		for (const Point &p : collection) {
			json << "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[" << p.x << "," << p.y << "]},\"properties\":{";

			for (auto &key : string_keys) {
				json << "\"" << key << "\":\"" << local_md_string.get(idx, key) << "\",";
			}

			for (auto &key : value_keys) {
				double value = local_md_value.get(idx, key);
				json << "\"" << key << "\":";
				if (std::isfinite(value)) {
					json << value;
				}
				else {
					json << "null";
				}

				json << ",";
			}

			if (has_time) {
				json << "\"time\":" << timestamps[idx] << ",";
			}

			json.seekp(((long) json.tellp()) - 1); // delete last ,
			json << "}},";
			idx++;
		}

		json.seekp(((long) json.tellp()) - 1); // delete last ,
		json << "]}";

	} else {

		//json << "{ \"type\": \"MultiPoint\", \"coordinates\": [ ";
		json << "{\"type\":\"FeatureCollection\",\"crs\": {\"type\": \"name\", \"properties\":{\"name\": \"EPSG:" << epsg <<"\"}},\"features\":[{\"type\":\"Feature\",\"geometry\":{\"type\": \"MultiPoint\", \"coordinates\": [ ";

		bool first = true;
		for (const Point &p : collection) {
			if (first)
				first = false;
			else
				json << ", ";

			json << "[" << p.x << "," << p.y << "]";
		}
		//json << "] }";
		json << "] }}]}";

	}

	return json.str();
}

std::string PointCollection::toCSV() {
	std::ostringstream csv;
	csv << std::fixed; // std::setprecision(4);

	auto string_keys = local_md_string.getKeys();
	auto value_keys = local_md_value.getKeys();

	//header
	csv << "lon" << "," << "lat";
	if (has_time)
		csv << ",\"time\"";
	for(auto &key : string_keys) {
		csv << ",\"" << key << "\"";
	}
	for(auto &key : value_keys) {
		csv << ",\"" << key << "\"";
	}
	csv << std::endl;

	size_t idx = 0;
	for (const auto &p : collection) {
		csv << p.x << "," << p.y;

		if (has_time)
			csv << "," << timestamps[idx];


		for(auto &key : string_keys) {
			csv << ",\"" << local_md_string.get(idx, key) << "\"";
		}
		for(auto &key : value_keys) {
			csv << "," << local_md_value.get(idx, key);
		}
		csv << std::endl;
		idx++;
	}

	return csv.str();
}

std::string PointCollection::hash() {
	// certainly not the most stable solution, but it has few lines of code..
	std::string csv = toCSV();

	return calculateHash((const unsigned char *) csv.c_str(), (int) csv.length()).asHex();
}
