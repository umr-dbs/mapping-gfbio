#include "multipointcollection.h"
#include "raster/exceptions.h"
#include "util/binarystream.h"
#include "util/hash.h"
#include "util/make_unique.h"

#include <sstream>
#include <iomanip>
#include <iostream>
#include <cmath>

template<typename T>
std::unique_ptr<MultiPointCollection> filter(MultiPointCollection *in, const std::vector<T> &keep) {
	size_t count = in->start_feature.size();
	if (keep.size() != count) {
		std::ostringstream msg;
		msg << "MultiPointCollection::filter(): size of filter does not match (" << keep.size() << " != " << count << ")";
		throw ArgumentException(msg.str());
	}

	size_t kept_count = 0;
	for (size_t idx=0;idx<count;idx++) {
		if (keep[idx])
			kept_count++;
	}

	auto out = std::make_unique<MultiPointCollection>(in->stref);
	out->start_feature.reserve(kept_count);
	// copy global metadata
	out->global_md_string = in->global_md_string;
	out->global_md_value = in->global_md_value;

	// copy features
	for (size_t featureIndex=0;featureIndex<count;featureIndex++) {
		if (keep[featureIndex]) {
			out->start_feature.push_back(out->coordinates.size());

			//copy coordinates
			for(size_t coordinateIndex = in->start_feature[featureIndex]; coordinateIndex < in->stopFeature(featureIndex); ++coordinateIndex){
				Coordinate &p = in->coordinates[coordinateIndex];
				out->addCoordinate(p.x, p.y);
			}
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

std::unique_ptr<MultiPointCollection> MultiPointCollection::filter(const std::vector<bool> &keep) {
	return ::filter<bool>(this, keep);
}

std::unique_ptr<MultiPointCollection> MultiPointCollection::filter(const std::vector<char> &keep) {
	return ::filter<char>(this, keep);
}

MultiPointCollection::MultiPointCollection(BinaryStream &stream) : SimpleFeatureCollection(stream) {
	size_t coordinateCount;
	stream.read(&coordinateCount);
	coordinates.reserve(coordinateCount);
	size_t featureCount;
	stream.read(&featureCount);
	start_feature.reserve(featureCount);

	global_md_string.fromStream(stream);
	global_md_value.fromStream(stream);
	local_md_string.fromStream(stream);
	local_md_value.fromStream(stream);

	for (size_t i=0;i<coordinateCount;i++) {
		coordinates.push_back( Coordinate(stream) );
	}

	uint32_t offset;
	for (size_t i=0;i<featureCount;i++) {
		stream.read(&offset);
		start_feature.push_back(offset);
	}

	// TODO: serialize/unserialize time array
}

void MultiPointCollection::toStream(BinaryStream &stream) {
	stream.write(stref);
	size_t coordinateCount = coordinates.size();
	stream.write(coordinateCount);
	size_t featureCount = start_feature.size();
	stream.write(featureCount);

	stream.write(global_md_string);
	stream.write(global_md_value);
	stream.write(local_md_string);
	stream.write(local_md_value);

	for (size_t i=0;i<coordinateCount;i++) {
		coordinates[i].toStream(stream);
	}

	for (size_t i=0;i<featureCount;i++) {
		stream.write(start_feature[i]);
	}

	// TODO: serialize/unserialize time array
}


void MultiPointCollection::addCoordinate(double x, double y) {
	coordinates.push_back(Coordinate(x, y));
}

size_t MultiPointCollection::addFeature(const std::vector<Coordinate> &featureCoordinates){
	start_feature.push_back(coordinates.size());
	coordinates.insert(coordinates.end(), featureCoordinates.begin(), featureCoordinates.end());

	return start_feature.size() - 1;
}

size_t MultiPointCollection::addFeature(Coordinate coordinate){
	start_feature.push_back(coordinates.size());
	coordinates.push_back(coordinate);
	return start_feature.size() - 1;
}


/**
 * Export
 */
#if 0
// http://www.gdal.org/ogr_apitut.html
#include "ogrsf_frmts.h"

void gdal_init(); // implemented in raster/import_gdal.cpp

void MultiPointCollection::toOGR(const char *driver = "ESRI Shapefile") {
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
	for (const Coordinate &p : collection) {
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

//TODO: include global metadata?
std::string MultiPointCollection::toGeoJSON(bool displayMetadata) {
	std::ostringstream json;
	json << std::fixed; // std::setprecision(4);

	json << "{\"type\":\"FeatureCollection\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:" << (int) stref.epsg <<"\"}},\"features\":[";

	size_t idx = 0;
	auto value_keys = local_md_value.getKeys();
	auto string_keys = local_md_string.getKeys();
	bool isSimpleCollection = isSimple();
	for (size_t index = 0; index < start_feature.size(); index++) {
		if(isSimpleCollection){
			//all features are single points
			Coordinate &p = coordinates[index];
			json << "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[" << p.x << "," << p.y << "]}";
		}
		else {
			json << "{\"type\":\"Feature\",\"geometry\":{\"type\":\"MultiPoint\",\"coordinates\":[";

			for(size_t coordinateIndex = start_feature[index]; coordinateIndex < stopFeature(index); ++coordinateIndex){
				Coordinate &p = coordinates[coordinateIndex];
				json << "[" << p.x << "," << p.y << "],";
			}
			json.seekp(((long) json.tellp()) - 1); // delete last ,

			json << "]}";
		}

		if(displayMetadata && (string_keys.size() > 0 || value_keys.size() > 0 || has_time)){
			json << ", properties\":{";

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
			json << "}";
		}
		json << "},";

	}

	json.seekp(((long) json.tellp()) - 1); // delete last ,
	json << "]}";

	return json.str();
}
//TODO: include global metadata?
std::string MultiPointCollection::toCSV() {
	std::ostringstream csv;
	csv << std::fixed; // std::setprecision(4);

	auto string_keys = local_md_string.getKeys();
	auto value_keys = local_md_value.getKeys();

	bool isSimpleCollection = isSimple();

	//header
	if(!isSimpleCollection){
		csv << "feature,";
	}
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
	size_t featureIndex = 0;
	for (const auto &p : coordinates) {
		if(!isSimpleCollection){
			if(idx >= stopFeature(featureIndex)){
				++featureIndex;
			}
			csv << featureIndex << ",";
		}
		csv << p.x << "," << p.y;

		if (has_time)
			csv << "," << timestamps[featureIndex];


		for(auto &key : string_keys) {
			csv << ",\"" << local_md_string.get(featureIndex, key) << "\"";
		}
		for(auto &key : value_keys) {
			csv << "," << local_md_value.get(featureIndex, key);
		}
		csv << std::endl;
		idx++;
	}

	return csv.str();
}

std::string MultiPointCollection::hash() {
	// certainly not the most stable solution, but it has few lines of code..
	std::string csv = toCSV();

	return calculateHash((const unsigned char *) csv.c_str(), (int) csv.length()).asHex();
}

bool MultiPointCollection::isSimple(){
	return coordinates.size() == start_feature.size();
}
