#ifndef RASTER_EXCEPTIONS_H
#define RASTER_EXCEPTIONS_H

#include <string>
#include <exception>

#include <iostream>

class RasterDBException : public std::exception {
	public:
		RasterDBException(const char *_msg, const char *_classname) {
			msg = std::string(_classname) + ": " + _msg;
			std::cerr << msg << std::endl;
		}
		virtual const char *what() const throw() {
			return msg.c_str();
		}
	private:
		std::string msg;
};

#define RASTERDB_EXCEPTION_CLASS(C) class C : public RasterDBException { public: C(const char *msg) : RasterDBException(msg, #C) {} C(std::string msg) : RasterDBException(msg.c_str(), #C) {}}

RASTERDB_EXCEPTION_CLASS(ArgumentException);
RASTERDB_EXCEPTION_CLASS(ImporterException);
RASTERDB_EXCEPTION_CLASS(ExporterException);
RASTERDB_EXCEPTION_CLASS(SourceException);
RASTERDB_EXCEPTION_CLASS(MetadataException);
RASTERDB_EXCEPTION_CLASS(ConverterException);
RASTERDB_EXCEPTION_CLASS(OperatorException);
RASTERDB_EXCEPTION_CLASS(OpenCLException);
RASTERDB_EXCEPTION_CLASS(PlatformException);
RASTERDB_EXCEPTION_CLASS(cURLException);
RASTERDB_EXCEPTION_CLASS(SQLiteException);

#undef RASTERDB_EXCEPTION_CLASS

#endif
