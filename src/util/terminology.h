
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
        static std::string request_label(const std::string &name, const std::string &terminology,
                                         const HandleNotResolvable onNotResolvable);

        static void request_labels(const std::vector<std::string> &names_in,
                                   std::vector<std::string> &names_out,
                                   const std::string &terminology,
                                   const HandleNotResolvable on_not_resolvable);

        static void requestLabelsAllConcurrent(const std::vector<std::string> &names_in,
                                               std::vector<std::string> &names_out,
                                               const std::string &terminology,
                                               const HandleNotResolvable on_not_resolvable);

        static void requestLabelsBatch(const std::vector<std::string> &names_in,
                                       std::vector<std::string> &names_out,
                                       const std::string &terminology,
                                       const HandleNotResolvable on_not_resolvable);

        static void request_labels_thread_pool(const std::vector<std::string> &names_in,
                                               std::vector<std::string> &names_out,
                                               const std::string &terminology,
                                               const HandleNotResolvable on_not_resolvable);

    private:
        static std::string resolve_single_name_set_session_ptr(Poco::Net::Context::Ptr &context,
                                                               Poco::Net::Session::Ptr *session_ptr,
                                                               const std::string &name,
                                                               const std::string &terminology,
                                                               const HandleNotResolvable on_not_resolvable);
    static std::string resolve_single_name(Poco::Net::Context::Ptr &context, Poco::Net::Session::Ptr &session_ptr,
                                           std::string &name, std::string &terminology,
                                           HandleNotResolvable &on_not_resolvable);
    static Json::Value get_https_response(Poco::URI uri);

};

#endif //MAPPING_CORE_TERMINOLOGY_H
