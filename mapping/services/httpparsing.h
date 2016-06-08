#ifndef SERVICES_HTTPPARSING_H
#define SERVICES_HTTPPARSING_H

#include "services/httpservice.h"

void parseQuery(const std::string& query, HTTPService::Params &params);
void parseGetData(HTTPService::Params &params);
void parsePostData(HTTPService::Params &params, std::istream &in);

#endif
