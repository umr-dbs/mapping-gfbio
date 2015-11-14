#include "datatypes/pointcollection.h"

#include "util/binarystream.h"
#include "util/hash.h"
#include "util/make_unique.h"

#include <sstream>
#include <iomanip>
#include <iostream>
#include <cmath>

template<typename T>
std::unique_ptr<PointCollection> filter(PointCollection *in, const std::vector<T> &keep) {
	size_t count = in->getFeatureCount();
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

	auto out = make_unique<PointCollection>(in->stref);
	out->start_feature.reserve(kept_count);
	// copy global metadata
	out->global_md_string = in->global_md_string;
	out->global_md_value = in->global_md_value;

	// copy features
	for (auto feature : *in) {
		if (keep[feature]) {
			//copy coordinates
			for (auto & c : feature) {
				out->addCoordinate(c.x, c.y);
			}
			out->finishFeature();
		}
	}

	// copy local MD
	for (auto &keyValue : in->local_md_string) {
		const auto &vec_in = in->local_md_string.getVector(keyValue.first);
		auto &vec_out = out->local_md_string.addEmptyVector(keyValue.first, kept_count);
		for (size_t idx=0;idx<count;idx++) {
			if (keep[idx])
				vec_out.push_back(vec_in[idx]);
		}
	}

	for (auto &keyValue : in->local_md_value) {
		const auto &vec_in = in->local_md_value.getVector(keyValue.first);
		auto &vec_out = out->local_md_value.addEmptyVector(keyValue.first, kept_count);
		for (size_t idx=0;idx<count;idx++) {
			if (keep[idx])
				vec_out.push_back(vec_in[idx]);
		}
	}
	// copy time arrays
	if (in->hasTime()) {
		out->time_start.reserve(kept_count);
		out->time_end.reserve(kept_count);
		for (auto i=0;i<count;i++) {
			for (size_t idx=0;idx<count;idx++) {
				if (keep[idx]) {
					out->time_start.push_back(in->time_start[idx]);
					out->time_end.push_back(in->time_end[idx]);
				}
			}
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

PointCollection::PointCollection(BinaryStream &stream) : SimpleFeatureCollection(stream) {
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

void PointCollection::toStream(BinaryStream &stream) const {
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


void PointCollection::addCoordinate(double x, double y) {
	coordinates.push_back(Coordinate(x, y));
}

size_t PointCollection::finishFeature(){
	if(start_feature.back() >= coordinates.size()){
		throw FeatureException("Tried to finish feature with 0 coordinates");
	}

	start_feature.push_back(coordinates.size());
	return start_feature.size() -2;
}

size_t PointCollection::addSinglePointFeature(Coordinate coordinate){
	coordinates.push_back(coordinate);
	start_feature.push_back(coordinates.size());
	return start_feature.size() -2;
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


void PointCollection::featureToGeoJSONGeometry(size_t featureIndex, std::ostringstream& json) const {
	auto feature = getFeatureReference(featureIndex);

	if(feature.size() == 1)
		json << "{\"type\":\"Point\",\"coordinates\":";
	else
		json << "{\"type\":\"MultiPoint\",\"coordinates\":[";

	for(auto& c : feature){
		json << "[" << c.x << "," << c.y << "],";
	}
	json.seekp(((long)json.tellp()) - 1); //delete last ,

	if(feature.size() > 1)
		json << "]";
	json << "}";
}

//TODO: include global metadata?
std::string PointCollection::toCSV() const {
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
	if (hasTime())
		csv << ",\"time_start\",\"time_end\"";
	for(auto &key : string_keys) {
		csv << ",\"" << key << "\"";
	}
	for(auto &key : value_keys) {
		csv << ",\"" << key << "\"";
	}
	csv << std::endl;

	for (auto feature : *this) {
		for (auto & c : feature) {
			if(!isSimpleCollection)
				csv << (size_t) feature << ",";
			csv << c.x << "," << c.y;

			if (hasTime())
				csv << "," << time_start[feature] << "," << time_end[feature];

			//TODO: handle missing metadata values
			for(auto &key : string_keys) {
				csv << ",\"" << local_md_string.get(feature, key) << "\"";
			}
			for(auto &key : value_keys) {
				csv << "," << local_md_value.get(feature, key);
			}
			csv << std::endl;
		}
	}

	return csv.str();
}

void PointCollection::featureToWKT(size_t featureIndex, std::ostringstream& wkt) const {
	if(featureIndex >= getFeatureCount()){
		throw ArgumentException("featureIndex is greater than featureCount");
	}

	auto feature = getFeatureReference(featureIndex);

	if(feature.size() == 1) {
		const Coordinate& coordinate = *feature.begin();
		wkt << "POINT(" << coordinate.x << " " << coordinate.y << ")";
	}
	else {
		wkt << "MULTIPOINT(";

		for(auto& coordinate : feature){
			wkt << "(" << coordinate.x << " " << coordinate.y << "),";
		}
		wkt.seekp(((long) wkt.tellp()) - 1); // delete last ,

		wkt << ")";
	}
}

std::string PointCollection::toARFF(std::string layerName) const {
	std::ostringstream arff;

	//TODO: maybe take name of layer as relation name, but this is not accessible here
	arff << "@RELATION " << layerName << std::endl << std::endl;

	bool isSimpleCollection = isSimple();

	if(!isSimpleCollection){
		arff << "@ATTRIBUTE feature NUMERIC" << std::endl;
	}
	arff << "@ATTRIBUTE longitude NUMERIC" << std::endl;
	arff << "@ATTRIBUTE latitude NUMERIC" << std::endl;

	if (hasTime()){
		arff << "@ATTRIBUTE time_start DATE" << std::endl;
		arff << "@ATTRIBUTE time_end DATE" << std::endl;
	}

	auto string_keys = local_md_string.getKeys();
	auto value_keys = local_md_value.getKeys();


	//TODO: handle missing metadata values
	for(auto &key : string_keys) {
		arff << "@ATTRIBUTE" << " " << key << " " << "STRING" << std::endl;
	}
	for(auto &key : value_keys) {
		arff << "@ATTRIBUTE" << " " << key << " " << "NUMERIC" << std::endl;
	}

	arff << std::endl;

	arff << "@DATA" << std::endl;

	for (auto feature : *this) {
		for (auto & c : feature) {
			if(!isSimpleCollection)
				arff << (size_t) feature << ",";
			arff << c.x << "," << c.y;

			if (hasTime()){
				arff << "," << "\"" << stref.toIsoString(time_start[feature]) << "\"" << ","
						 << "\"" << stref.toIsoString(time_end[feature]) << "\"";
			}

			for(auto &key : string_keys) {
				arff << ",\"" << local_md_string.get(feature, key) << "\"";
			}
			for(auto &key : value_keys) {
				arff << "," << local_md_value.get(feature, key);
			}
			arff << std::endl;
		}
	}

	return arff.str();
}

std::string PointCollection::hash() {
	// certainly not the most stable solution, but it has few lines of code..
	std::string csv = toCSV();

	return calculateHash((const unsigned char *) csv.c_str(), (int) csv.length()).asHex();
}

bool PointCollection::isSimple() const {
	return coordinates.size() == getFeatureCount();
}

std::string PointCollection::getAsString(){
	std::ostringstream string;

	string << "points" << std::endl;
	for(auto p = coordinates.begin(); p !=coordinates.end(); ++p){
		string << (*p).x << "," << (*p).y << ' ';
	}

	string << std::endl;
	string << "features" << std::endl;
	for(auto p = start_feature.begin(); p != start_feature.end(); ++p){
		string << *p << ' ';
	}

	return string.str();
}

SpatialReference PointCollection::getFeatureMBR(size_t featureIndex) const{
	return getFeatureReference(featureIndex).getMBR();
}

void PointCollection::validateSpecifics() const {
	if(start_feature.back() != coordinates.size())
		throw FeatureException("Feature not finished");
}
