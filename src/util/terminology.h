
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


/**
 * Class wrapping calls to terminologies service from gfbio.
 */
class Terminology {
    public:
        /**
         * Resolving a single string
         * @param name string to be resolved
         * @param terminology name of terminology used to resolving
         * @param key the field in the result json from terminologies taken as a result
         * @param on_not_resolvable how to handle a not resolvable string: EMPTY or OLD_NAME
         * @return the resovled string
         */
        static std::string resolveSingle(const std::string &name,
                                         const std::string &terminology,
                                         const std::string &key,
                                         const HandleNotResolvable onNotResolvable);

        /**
         * Resolve a vector of strings
         * @param names_in vector of strings to be resolved
         * @param names_out vector to push resolved strings into
         * @param terminology name of the terminology used
         * @param key the field in the result json from terminologies taken as a result
         * @param on_not_resolvable how to handle a not resolvable string: EMPTY or OLD_NAME
         */
        static void resolveMultiple(const std::vector<std::string> &names_in,
                                    std::vector<std::string> &names_out,
                                    const std::string &terminology,
                                    const std::string &key,
                                    const HandleNotResolvable on_not_resolvable);

    private:
        typedef std::pair<std::string, std::string> string_pair;
        typedef std::pair<Terminology::string_pair, Poco::Net::Session::Ptr> string_session_ptr_pair;

        static string_session_ptr_pair resolveSingleNameSetSessionPtr(Poco::Net::Context::Ptr &context,
                                                                      const std::string &name,
                                                                      const std::string &terminology,
                                                                      const std::string &key,
                                                                      const HandleNotResolvable on_not_resolvable);

        static string_pair resolveSingleNameInternal(Poco::Net::Context::Ptr &context,
                                                     Poco::Net::Session::Ptr &session_ptr,
                                                     const std::string &name,
                                                     const std::string &terminology,
                                                     const std::string &key,
                                                     const HandleNotResolvable &on_not_resolvable);
};

#endif //MAPPING_CORE_TERMINOLOGY_H
