

#ifndef ***REMOVED***_hpp

namespace ***REMOVED*** {
	// QueryRectangle
	template<> SEXP wrap(const QueryRectangle &rect);
	template<> QueryRectangle as(SEXP sexp);

	// Raster
	template<> SEXP wrap(const GenericRaster &raster);
	template<> SEXP wrap(const std::unique_ptr<GenericRaster> &raster);
	template<> std::unique_ptr<GenericRaster> as(SEXP sexp);

	// PointCollection
	template<> SEXP wrap(const PointCollection &points);
	template<> SEXP wrap(const std::unique_ptr<PointCollection> &points);
	template<> std::unique_ptr<PointCollection> as(SEXP sexp);

}

#else

namespace ***REMOVED*** {
	// QueryRectangle
	template<> SEXP wrap(const QueryRectangle &rect) {
		Profiler::Profiler p("***REMOVED***: wrapping qrect");
		***REMOVED***::List list;

		list["t1"] = rect.t1;
		list["t2"] = rect.t2;
		list["x1"] = rect.x1;
		list["y1"] = rect.y1;
		list["x2"] = rect.x2;
		list["y2"] = rect.y2;
		if (rect.restype == QueryResolution::Type::PIXELS) {
			list["xres"] = rect.xres;
			list["yres"] = rect.xres;
		}
		else if (rect.restype == QueryResolution::Type::NONE) {
			list["xres"] = 0;
			list["yres"] = 0;
		}
		else
			throw ArgumentException("***REMOVED***::wrap(): cannot convert a QueryRectangle with unknown resolution type");
		list["epsg"] = (int) rect.epsg;

		return ***REMOVED***::wrap(list);
	}
	template<> QueryRectangle as(SEXP sexp) {
		Profiler::Profiler p("***REMOVED***: unwrapping qrect");
		***REMOVED***::List list = ***REMOVED***::as<***REMOVED***::List>(sexp);

		int xres = list["xres"];
		int yres = list["yres"];

		return QueryRectangle(
			SpatialReference((epsg_t) (int) list["epsg"], list["x1"], list["y1"], list["x2"], list["y2"]),
			TemporalReference(TIMETYPE_UNIX, list["t1"], list["t2"]),
			(xres > 0 && yres > 0) ? QueryResolution::pixels(xres, yres) : QueryResolution::none()
		);
	}

	// Raster
	template<> SEXP wrap(const GenericRaster &raster) {
		/*
		class	   : RasterLayer
		dimensions  : 180, 360, 64800  (nrow, ncol, ncell)
		resolution  : 1, 1  (x, y)
		extent	  : -180, 180, -90, 90  (xmin, xmax, ymin, ymax)
		coord. ref. : +proj=longlat +datum=WGS84

		attributes(r)
		$history: list()
		$file: class .RasterFile
		$data: class .SingleLayerData  (hat haveminmax, min, max!)
		$legend: class .RasterLegend
		$title: character(0)
		$extent: class Extent (xmin, xmax, ymin, ymax)
		$rotated: FALSE
		$rotation: class .Rotation
		$ncols: int
		$nrows: int
		$crs: CRS arguments: +proj=longlat +datum=WGS84
		$z: list()
		$class: c("RasterLayer", "raster")

		 */
		Profiler::Profiler p("***REMOVED***: wrapping raster");
		int width = raster.width;
		int height = raster.height;

		***REMOVED***::NumericVector pixels(raster.getPixelCount());
		int pos = 0;
		for (int y=0;y<height;y++) {
			for (int x=0;x<width;x++) {
				double val = raster.getAsDouble(x, y);
				if (raster.dd.is_no_data(val))
					pixels[pos++] = NAN;
				else
					pixels[pos++] = val;
			}
		}

		***REMOVED***::S4 data(".SingleLayerData");
		data.slot("values") = pixels;
		data.slot("inmemory") = true;
		data.slot("fromdisk") = false;
		data.slot("haveminmax") = true;
		data.slot("min") = raster.dd.min;
		data.slot("max") = raster.dd.max;

		// TODO: how exactly would R like the Extent to be?
		***REMOVED***::S4 extent("Extent");
		extent.slot("xmin") = raster.stref.x1;
		extent.slot("ymin") = raster.stref.y1;
		extent.slot("xmax") = raster.stref.x2;
		extent.slot("ymax") = raster.stref.y2;

		***REMOVED***::S4 crs("CRS");
		std::ostringstream epsg;
		epsg << "EPSG:" << (int) raster.stref.epsg;
		crs.slot("projargs") = epsg.str();

		***REMOVED***::S4 rasterlayer("RasterLayer");
		rasterlayer.slot("data") = data;
		rasterlayer.slot("extent") = extent;
		rasterlayer.slot("crs") = crs;
		rasterlayer.slot("ncols") = raster.width;
		rasterlayer.slot("nrows") = raster.height;

		return ***REMOVED***::wrap(rasterlayer);
	}
	template<> SEXP wrap(const std::unique_ptr<GenericRaster> &raster) {
		return ***REMOVED***::wrap(*raster);
	}
	template<> std::unique_ptr<GenericRaster> as(SEXP sexp) {
		Profiler::Profiler p("***REMOVED***: unwrapping raster");
		***REMOVED***::S4 rasterlayer(sexp);
		if (!rasterlayer.is("RasterLayer"))
			throw OperatorException("Result is not a RasterLayer");

		int width = rasterlayer.slot("ncols");
		int height = rasterlayer.slot("nrows");

		***REMOVED***::S4 crs = rasterlayer.slot("crs");
		std::string epsg_string = crs.slot("projargs");
		epsg_t epsg = EPSG_UNKNOWN;
		if (epsg_string.compare(0,5,"EPSG:") == 0)
			epsg = (epsg_t) std::stoi(epsg_string.substr(5, std::string::npos));
		if (epsg == EPSG_UNKNOWN)
			throw OperatorException("Result raster has no projection of form EPSG:1234 set");

		***REMOVED***::S4 extent = rasterlayer.slot("extent");
		double xmin = extent.slot("xmin"), ymin = extent.slot("ymin"), xmax = extent.slot("xmax"), ymax = extent.slot("ymax");

		SpatioTemporalReference stref(
			SpatialReference(epsg, xmin, ymin, xmax, ymax),
			TemporalReference::unreferenced()
		);

		***REMOVED***::S4 data = rasterlayer.slot("data");
		if ((bool) data.slot("inmemory") != true)
			throw OperatorException("Result raster not inmemory");
		if ((bool) data.slot("haveminmax") != true)
			throw OperatorException("Result raster does not have min/max");

		double min = data.slot("min");
		double max = data.slot("max");

		DataDescription dd(GDT_Float32, min, max, true, NAN);
		dd.verify();
		auto raster_out = GenericRaster::create(dd, stref, width, height, GenericRaster::Representation::CPU);
		Raster2D<float> *raster2d = (Raster2D<float> *) raster_out.get();

		***REMOVED***::NumericVector pixels = data.slot("values");
		int pos = 0;
		for (int y=0;y<height;y++) {
			for (int x=0;x<width;x++) {
				float val = pixels[pos++];
				raster2d->set(x, y, val);
			}
		}
		return raster_out;
	}


	// PointCollection
	template<> SEXP wrap(const PointCollection &points) {
		/*
		new("SpatialPointsDataFrame"
			, data = structure(list(), .Names = character(0), row.names = integer(0), class = "data.frame")
			, coords.nrs = numeric(0)
			, coords = structure(NA, .Dim = c(1L, 1L))
			, bbox = structure(NA, .Dim = c(1L, 1L))
			, proj4string = new("CRS"
			, projargs = NA_character_
		)
		*/
		Profiler::Profiler p("***REMOVED***: wrapping pointcollection");
		***REMOVED***::S4 SPDF("SpatialPointsDataFrame");

		auto size = points.coordinates.size();

		***REMOVED***::DataFrame data;
		auto numeric_keys = points.local_md_value.getKeys();
		for(auto key : numeric_keys) {
			***REMOVED***::NumericVector vec(size);
			for (decltype(size) i=0;i<size;i++) {
				double value = points.local_md_value.get(i, key);
				vec[i] = value;
			}
			data[key] = vec;
		}

		auto string_keys = points.local_md_string.getKeys();
		for(auto key : string_keys) {
			***REMOVED***::StringVector vec(size);
			for (decltype(size) i=0;i<size;i++) {
				auto &value = points.local_md_string.get(i, key);
				vec[i] = value;
			}
			data[key] = vec;
		}


		***REMOVED***::NumericMatrix coords(size, 2);
		for (decltype(size) i=0;i<size;i++) {
			const Coordinate &p = points.coordinates[i];
			coords(i, 0) = p.x;
			coords(i, 1) = p.y;
		}

		// TODO: convert time vector

		***REMOVED***::NumericMatrix bbox(2,2); // TODO ?

		***REMOVED***::S4 crs("CRS");
		std::ostringstream epsg;
		epsg << "EPSG:" << (int) points.stref.epsg;
		crs.slot("projargs") = epsg.str();


		SPDF.slot("data") = data;
		SPDF.slot("coords.nrs") = true;
		SPDF.slot("coords") = coords;
		SPDF.slot("bbox") = bbox;
		SPDF.slot("proj4string") = crs;

		return SPDF;
	}
	template<> SEXP wrap(const std::unique_ptr<PointCollection> &points) {
		return ***REMOVED***::wrap(*points);
	}
	template<> std::unique_ptr<PointCollection> as(SEXP sexp) {
		Profiler::Profiler p("***REMOVED***: unwrapping pointcollection");
		***REMOVED***::S4 SPDF(sexp);
		if (!SPDF.is("SpatialPointsDataFrame"))
			throw OperatorException("Result is not a SpatialPointsDataFrame");

		bool nrs = ***REMOVED***::as<bool>(SPDF.slot("coords.nrs"));
		if (nrs != true)
			throw OperatorException("Result has nrs = false, cannot convert");

		***REMOVED***::S4 crs = SPDF.slot("proj4string");
		std::string epsg_s = crs.slot("projargs");

		if (epsg_s.compare(0,5,"EPSG:") != 0)
			throw OperatorException("Result has an unknown epsg");
		epsg_t epsg = (epsg_t) std::stoi(epsg_s.substr(5, std::string::npos));

		auto points = make_unique<PointCollection>(SpatioTemporalReference(epsg, TIMETYPE_UNIX));

		***REMOVED***::NumericMatrix coords = ***REMOVED***::as<***REMOVED***::NumericMatrix>(SPDF.slot("coords"));

		size_t size = coords.nrow();
		points->coordinates.reserve(size);
		for (size_t i=0;i<size;i++) {
			double x = coords(i, 0);
			double y = coords(i, 1);
			points->addSinglePointFeature(Coordinate(x, y));
		}

		***REMOVED***::DataFrame data = ***REMOVED***::as<***REMOVED***::DataFrame>(SPDF.slot("data"));
		***REMOVED***::List attrs = (*attributes)(data);
		***REMOVED***::StringVector a = attrs["names"];
		auto len = a.length();
		for (int i=0;i<len;i++) {
			std::string attr = ***REMOVED***::as<std::string>(a[i]);
			try {
				***REMOVED***::NumericVector rvec = data[attr];
				auto & vec = points->local_md_value.addVector(attr, size);
				for (size_t i=0;i<size;i++)
					vec[i] = rvec[i];
			}
			catch (const ***REMOVED***::not_compatible &e) {
				***REMOVED***::StringVector rvec = data[attr];
				auto & vec = points->local_md_string.addVector(attr, size);
				for (size_t i=0;i<size;i++)
					vec[i] = rvec[i];
			}

			LOG("Attribute %d: %s", i, attr.c_str());
		}

		// TODO: convert time vector

		return points;
	}

}

#endif
