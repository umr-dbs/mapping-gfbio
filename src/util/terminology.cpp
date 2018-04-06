
#include "terminology.h"
#include <vector>
#include <iostream>
#include <future>
#include "util/make_unique.h"
#include "util/configuration.h"

#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <boost/move/move.hpp>

std::vector<std::string> Terminology::resolveMultiple(const std::vector<std::string> &names_in,
                                                      const std::string &terminology,
                                                      const std::string &key,
                                                      const std::string &match_type,
                                                      const bool first_hit,
                                                      const HandleNotResolvable on_not_resolvable){

    std::vector<std::string> names_out;
    if(names_in.empty())
        return names_out;
    names_out.reserve(names_in.size());

    //get a set with all names to be resolved (so we don't request the same name multiple times)
    std::set<std::string> to_resolve;
    std::map<std::string, std::string> resolved_pairs;

    for(auto &name : names_in){
        to_resolve.insert(name);
    }

    //aliases only needed in this method.
    using task_t  = boost::packaged_task<string_pair>;
    using ptask_t = boost::shared_ptr<task_t>;

    //init thread pool
    int threads_num = Configuration::get<int>("terminology.threads",16);
    if(threads_num > to_resolve.size())
        threads_num = to_resolve.size();

    boost::asio::io_service io_service;
    boost::thread_group threads;
    boost::asio::io_service::work work(io_service);

    for(int i = 0; i < threads_num; i++){
        threads.create_thread(boost::bind(&boost::asio::io_service::run, &io_service));
    }

    std::vector<boost::shared_future<string_pair>> pending_results;

    // create context, enable session cache. First name has to be resolved separately
    // and not in a thread to retrieve the session ptr.
    Poco::Net::Context::Ptr context = new Poco::Net::Context(
            Poco::Net::Context::CLIENT_USE,
            "",
            Poco::Net::Context::VERIFY_RELAXED,
            9,
            true
    );
    context->enableSessionCache(true);

    // declare iterator here to take first element, resolve it no in thread
    auto begin = to_resolve.begin();

    //pair<string_pair, Session::Ptr>
    auto first_resolved = resolveSingleNameSetSessionPtr(context, *begin, terminology, key, match_type, first_hit, on_not_resolvable);
    Poco::Net::Session::Ptr session_ptr = first_resolved.second;
    resolved_pairs.insert(first_resolved.first);

    // move iterator to next element
    begin++;

    // use iterator to push tasks for all names left into the thread pool
    for(auto it = begin; it != to_resolve.end(); it++) {
        const std::string &name = *it;
        ptask_t task = boost::make_shared<task_t>(
                boost::bind(resolveSingleNameInternal, context, session_ptr, name, terminology, key, match_type, first_hit, on_not_resolvable));
        boost::shared_future<string_pair> future(task->get_future());
        pending_results.push_back(future);
        io_service.post(boost::bind(&task_t::operator(), task));
    }

    //get the resolved pairs from the futures, insert into map
    for(auto &future : pending_results){
        resolved_pairs.insert(future.get());
    }

    //insert values from resolved pairs into names_out
    for(auto &name : names_in){
        names_out.push_back(resolved_pairs[name]);
    }
    
}

/**
 *
 * @return a pair of strings, first string is the string to be resolved and the second is the resolved string.
 */
Terminology::string_pair Terminology::resolveSingleNameInternal(Poco::Net::Context::Ptr &context,
                                                   Poco::Net::Session::Ptr &session_ptr,
                                                   const std::string &name,
                                                   const std::string &terminology,
                                                   const std::string &key,
                                                   const std::string &match_type,
                                                   const bool first_hit,
                                                   const HandleNotResolvable &on_not_resolvable)
{
    std::string uri_string = Configuration::get<std::string>("terminology.url_search");
    Poco::URI uri(uri_string);
    uri.addQueryParameter("query", name);
    uri.addQueryParameter("terminologies", terminology);
    uri.addQueryParameter("match_type", match_type);
    if(first_hit)
        uri.addQueryParameter("first_hit", "true");

    Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort(), context, session_ptr);
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
    Poco::Net::HTTPResponse response;

    session.sendRequest(request);
    std::istream& respStream = session.receiveResponse(response);

    //read response stream into json object
    Json::Value response_json;
    if (response.getStatus() == Poco::Net::HTTPResponse::HTTP_OK)
    {
        respStream >> response_json;
    } else {
        response_json = Json::Value::null;
    }

    //retrieve wanted element from result json, if not valid result return not_resolved.
    std::string not_resolved = (on_not_resolvable == HandleNotResolvable::EMPTY) ? "" : name;
    if (response_json.isNull())
    {
        return std::make_pair(name, not_resolved);
    } else
    {
        Json::Value results = response_json["results"];
        if(results.empty())
            return std::make_pair(name, not_resolved);

        Json::Value val = results[0].get(key, not_resolved);
        if(val.isArray()){
            if(val.empty())
                return std::make_pair(name, not_resolved);
            else
                return std::make_pair(name, val[0].asString());
        }
        else {
            return std::make_pair(name, val.asString());
        }
    }
}

/**
 * Same method as resolveSingleNameInternal, but also return the session_ptr to the sslSession of the created HttpsClientSession.
 * @return a pair, first element is a string_pair as in resolveSingleNameInternal, second element is the Session::Ptr.
 */
Terminology::string_session_ptr_pair
Terminology::resolveSingleNameSetSessionPtr(Poco::Net::Context::Ptr &context,
                                            const std::string &name,
                                            const std::string &terminology,
                                            const std::string &key,
                                            const std::string &match_type,
                                            const bool first_hit,
                                            const HandleNotResolvable on_not_resolvable) {

    std::string uri_string = Configuration::get<std::string>("terminology.url_search");
    Poco::URI uri(uri_string);
    uri.addQueryParameter("query", name);
    uri.addQueryParameter("terminologies", terminology);
    uri.addQueryParameter("match_type", match_type);
    if(first_hit)
        uri.addQueryParameter("first_hit", "true");

    Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort(), context);
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
    Poco::Net::HTTPResponse response;

    session.sendRequest(request);
    std::istream& respStream = session.receiveResponse(response);
    Json::Value response_json;

    if (response.getStatus() == Poco::Net::HTTPResponse::HTTP_OK)
    {
        respStream >> response_json;
    } else {
        response_json = Json::Value::null;
    }

    std::string not_resolved = (on_not_resolvable == HandleNotResolvable::EMPTY) ? "" : name;

    if (response_json.isNull())
    {
        return std::make_pair(std::make_pair(name, not_resolved), session.sslSession());
    } else
    {
        Json::Value results = response_json["results"];
        if(results.empty())
            return std::make_pair(std::make_pair(name, not_resolved), session.sslSession());

        Json::Value val = results[0].get(key, not_resolved);
        if(val.isArray()){
            if(val.empty())
                return std::make_pair(std::make_pair(name, not_resolved), session.sslSession());
            else
                return std::make_pair(std::make_pair(name, val[0].asString()), session.sslSession());
        }
        else {
            return std::make_pair(std::make_pair(name, val.asString()), session.sslSession());
        }
    }
}

std::string Terminology::resolveSingle(const std::string &name,
                                       const std::string &terminology,
                                       const std::string &key,
                                       const std::string &match_type,
                                       const bool first_hit,
                                       const HandleNotResolvable onNotResolvable)
{
    std::string uri_string = Configuration::get<std::string>("terminology.url_search");
    Poco::URI uri(uri_string);
    uri.addQueryParameter("query", name);
    uri.addQueryParameter("terminologies", terminology);
    uri.addQueryParameter("match_type", match_type);
    if(first_hit)
        uri.addQueryParameter("first_hit", "true");

    Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort());
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
    Poco::Net::HTTPResponse response;

    session.sendRequest(request);
    std::istream& respStream = session.receiveResponse(response);

    Json::Value response_json;
    if (response.getStatus() == Poco::Net::HTTPResponse::HTTP_OK)
    {
        respStream >> response_json;
    } else {
        response_json = Json::Value::null;
    }

    std::string not_resolved = (onNotResolvable == HandleNotResolvable::EMPTY) ? "" : name;
    if (response_json.isNull())
    {
        return not_resolved;
    } else
    {
        Json::Value results = response_json["results"];
        if(results.empty())
            return not_resolved;

        Json::Value val = results[0].get(key, not_resolved);
        if(val.isArray()){
            if(val.empty())
                return not_resolved;
            else
                return val[0].asString();
        }
        else {
            return val.asString();
        }
    }
}
