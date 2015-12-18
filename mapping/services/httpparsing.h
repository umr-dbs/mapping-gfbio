#ifndef SERVICES_HTTPPARSING_H
#define SERVICES_HTTPPARSING_H

#include "services/httpservice.h"

void parseGetData(HTTPService::Params &params);
void parsePostData(HTTPService::Params &params, std::istream &in);

#endif
