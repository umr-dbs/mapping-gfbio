
#ifndef MAPPING_CORE_TERMINOLOGY_H
#define MAPPING_CORE_TERMINOLOGY_H

#include <string>
#include <json/json.h>
#include <Poco/URI.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include "datatypes/simplefeaturecollection.h"

enum HandleNotResolvable {
    EMPTY,
    OLD_NAME
};

class Terminology {
    public:
        static std::string requestSingleLabel(const std::string &name,
                                              const std::string &terminology,
                                              HandleNotResolvable onNotResolvable);

        static void requestLabels(const std::vector<std::string> &names_in,
                                  std::vector<std::string> &names_out,
                                  const std::string &terminology,
                                  HandleNotResolvable on_not_resolvable);

    private:
        static std::string resolveSingleNameSetSessionPtr(Poco::Net::Context::Ptr &context,
                                                          Poco::Net::Session::Ptr *session_ptr,
                                                          const std::string &name,
                                                          const std::string &terminology,
                                                          HandleNotResolvable on_not_resolvable);

        static std::string resolveSingleName(Poco::Net::Context::Ptr &context, Poco::Net::Session::Ptr &session_ptr,
                                             std::string &name, std::string &terminology,
                                             HandleNotResolvable &on_not_resolvable);
};

#endif //MAPPING_CORE_TERMINOLOGY_H
