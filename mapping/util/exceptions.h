#ifndef UTIL_EXCEPTIONS_H
#define UTIL_EXCEPTIONS_H

#include "util/concat.h"

#include <stdexcept>


#define _CUSTOM_EXCEPTION_CLASS_PARENT(C, PARENT) class C : public PARENT { public: C(const std::string &msg) : PARENT(#C ": " + msg) {}}

// This is just some magic to get around the fact that macros cannot have default parameters
#define _CUSTOM_EXCEPTION_CLASS_DEFAULT(C) _CUSTOM_EXCEPTION_CLASS_PARENT(C, std::runtime_error)

#define _CUSTOM_EXCEPTION_CLASS_GET_3RD_PARAM(p1, p2, p3, ...) p3
#define _CUSTOM_EXCEPTION_CLASS_CHOOSER(...) _CUSTOM_EXCEPTION_CLASS_GET_3RD_PARAM(__VA_ARGS__, _CUSTOM_EXCEPTION_CLASS_PARENT, _CUSTOM_EXCEPTION_CLASS_DEFAULT, 0)

#define _CUSTOM_EXCEPTION_CLASS(...) _CUSTOM_EXCEPTION_CLASS_CHOOSER(__VA_ARGS__)(__VA_ARGS__)


_CUSTOM_EXCEPTION_CLASS(ArgumentException);
_CUSTOM_EXCEPTION_CLASS(ImporterException);
_CUSTOM_EXCEPTION_CLASS(ExporterException);
_CUSTOM_EXCEPTION_CLASS(MetadataException);
_CUSTOM_EXCEPTION_CLASS(AttributeException);
_CUSTOM_EXCEPTION_CLASS(ConverterException);
_CUSTOM_EXCEPTION_CLASS(OperatorException);
_CUSTOM_EXCEPTION_CLASS(SourceException, OperatorException);
_CUSTOM_EXCEPTION_CLASS(OpenCLException);
_CUSTOM_EXCEPTION_CLASS(PlatformException);
_CUSTOM_EXCEPTION_CLASS(cURLException);
_CUSTOM_EXCEPTION_CLASS(SQLiteException);
_CUSTOM_EXCEPTION_CLASS(GDALException);
_CUSTOM_EXCEPTION_CLASS(NetworkException);
_CUSTOM_EXCEPTION_CLASS(FeatureException);
_CUSTOM_EXCEPTION_CLASS(TimeParseException);

// Added Micha
_CUSTOM_EXCEPTION_CLASS(CacheException);
_CUSTOM_EXCEPTION_CLASS(NoSuchElementException, CacheException);
_CUSTOM_EXCEPTION_CLASS(NotInitializedException, CacheException);
_CUSTOM_EXCEPTION_CLASS(TimeoutException, CacheException);
_CUSTOM_EXCEPTION_CLASS(InterruptedException, CacheException);
_CUSTOM_EXCEPTION_CLASS(DeliveryException, CacheException);
_CUSTOM_EXCEPTION_CLASS(IllegalStateException, CacheException);



#undef _CUSTOM_EXCEPTION_CLASS
#undef _CUSTOM_EXCEPTION_CLASS_CHOOSER
#undef _CUSTOM_EXCEPTION_CLASS_GET_3RD_PARAM
#undef _CUSTOM_EXCEPTION_CLASS_DEFAULT
#undef _CUSTOM_EXCEPTION_CLASS_PARENT

#endif
