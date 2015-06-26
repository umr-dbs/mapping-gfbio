#ifndef RASTER_EXCEPTIONS_H
#define RASTER_EXCEPTIONS_H

#include <string>
#include <exception>
#include <stdexcept>

#include <iostream>


#define _CUSTOM_EXCEPTION_CLASS(C) class C : public std::runtime_error { public: C(const std::string &msg) : std::runtime_error(std::string(#C) + ": " + msg) {}}

_CUSTOM_EXCEPTION_CLASS(ArgumentException);
_CUSTOM_EXCEPTION_CLASS(ImporterException);
_CUSTOM_EXCEPTION_CLASS(ExporterException);
_CUSTOM_EXCEPTION_CLASS(SourceException);
_CUSTOM_EXCEPTION_CLASS(MetadataException);
_CUSTOM_EXCEPTION_CLASS(ConverterException);
_CUSTOM_EXCEPTION_CLASS(OperatorException);
_CUSTOM_EXCEPTION_CLASS(OpenCLException);
_CUSTOM_EXCEPTION_CLASS(PlatformException);
_CUSTOM_EXCEPTION_CLASS(cURLException);
_CUSTOM_EXCEPTION_CLASS(SQLiteException);
_CUSTOM_EXCEPTION_CLASS(GDALException);
_CUSTOM_EXCEPTION_CLASS(NetworkException);
_CUSTOM_EXCEPTION_CLASS(FeatureException);
// Added Micha
_CUSTOM_EXCEPTION_CLASS(NoSuchElementException);
_CUSTOM_EXCEPTION_CLASS(NotInitializedException);
_CUSTOM_EXCEPTION_CLASS(TimeoutException);
_CUSTOM_EXCEPTION_CLASS(InterruptedException);
_CUSTOM_EXCEPTION_CLASS(DeliveryException);
_CUSTOM_EXCEPTION_CLASS(IllegalStateException);



#undef _CUSTOM_EXCEPTION_CLASS

#endif
