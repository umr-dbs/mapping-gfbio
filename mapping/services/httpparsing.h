#ifndef SERVICES_HTTPPARSING_H
#define SERVICES_HTTPPARSING_H

#include "services/httpservice.h"

void parseQuery(const std::string& query, Parameters &params);
void parseGetData(Parameters &params);
void parsePostData(Parameters &params, std::istream &in);

#endif
