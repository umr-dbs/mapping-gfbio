
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
        static std::string requestLabel(const std::string &name, const std::string &terminology, const HandleNotResolvable onNotResolvable);

        static void requestLabels(const std::vector<std::string> &names_in,
                                  std::vector<std::string> &names_out, const std::string &terminology,
                                  const HandleNotResolvable on_not_resolvable);

        static void requestLabelsAllConcurrent(const std::vector<std::string> &names_in,
                                               std::vector<std::string> &names_out, const std::string &terminology,
                                               const HandleNotResolvable on_not_resolvable);
        static void requestLabelsBatch(const std::vector<std::string> &names_in,
                                       std::vector<std::string> &names_out, const std::string &terminology,
                                       const HandleNotResolvable on_not_resolvable);
        static void requestLabelsThreadPool(const std::vector<std::string> &names_in,
                                            std::vector<std::string> &names_out, const std::string &terminology,
                                            const HandleNotResolvable on_not_resolvable);

    private:
        static Json::Value getHttpsResponse(Poco::URI uri);

};

#endif //MAPPING_CORE_TERMINOLOGY_H
