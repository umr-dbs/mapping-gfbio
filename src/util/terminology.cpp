
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

typedef boost::packaged_task<std::string> task_t;
typedef boost::shared_ptr<task_t> ptask_t;

void Terminology::requestLabels(const std::vector<std::string> &names_in,
                                std::vector<std::string> &names_out,
                                const std::string &terminology,
                                const HandleNotResolvable on_not_resolvable){

    if(names_in.empty())
        return;

    //init thread pool
    const int threads_num = Configuration::get<int>("terminology.threads",16);

    boost::asio::io_service io_service;
    boost::thread_group threads;
    boost::asio::io_service::work work(io_service);

    for(int i = 0; i < threads_num; i++){
        threads.create_thread(boost::bind(&boost::asio::io_service::run, &io_service));
    }

    std::vector<boost::shared_future<std::string>> pending_results;

    // create context, enable session cache. First name has to be resolved seperately and not in thread to
    // set the session ptr. Because the Session::Ptr can not be assigned a new value the local session_ptr
    // variable is passed as a raw pointer to the method.
    Poco::Net::Context::Ptr context = new Poco::Net::Context(
            Poco::Net::Context::CLIENT_USE,
            "",
            Poco::Net::Context::VERIFY_RELAXED,
            9,
            true
    );
    context->enableSessionCache(true);
    Poco::Net::Session::Ptr session_ptr;
    std::string first_resolved = resolveSingleNameSetSessionPtr(context, &session_ptr, names_in[0], terminology,
                                                                on_not_resolvable);
    names_out.push_back(first_resolved);

    //push tasks to resolve all names into the thread pool
    for(int i = 1; i < names_in.size(); i++) {
        const std::string &name = names_in[i];
        ptask_t task = boost::make_shared<task_t>(
                boost::bind(resolveSingleName, context, session_ptr, name, terminology, on_not_resolvable));
        boost::shared_future<std::string> future(task->get_future());
        pending_results.push_back(future);
        io_service.post(boost::bind(&task_t::operator(), task));
    }

    //get the results from the futures
    for(auto &future : pending_results){
        names_out.push_back(future.get());
    }}


std::string Terminology::resolveSingleName(Poco::Net::Context::Ptr &context, Poco::Net::Session::Ptr &session_ptr,
                                           std::string &name, std::string &terminology,
                                           HandleNotResolvable &on_not_resolvable){
    Poco::URI uri("https://terminologies.gfbio.org/api/terminologies/search");
    uri.addQueryParameter("query", name);
    uri.addQueryParameter("terminologies", terminology);

    Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort(), context, session_ptr);
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

    std::string not_resolved = (on_not_resolvable == EMPTY) ? "" : name;

    if (response_json.isNull())
    {
        return not_resolved;
    } else
    {
        Json::Value results = response_json["results"];
        Json::Value first = results[0];
        return first.get("label", not_resolved).asString();
    }
}

std::string Terminology::resolveSingleNameSetSessionPtr(Poco::Net::Context::Ptr &context,
                                                        Poco::Net::Session::Ptr *session_ptr,
                                                        const std::string &name,
                                                        const std::string &terminology,
                                                        HandleNotResolvable on_not_resolvable) {

    Poco::URI uri("https://terminologies.gfbio.org/api/terminologies/search");
    uri.addQueryParameter("query", name);
    uri.addQueryParameter("terminologies", terminology);

    Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort(), context);
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
    Poco::Net::HTTPResponse response;

    session.sendRequest(request);

    *session_ptr = session.sslSession();

    std::istream& respStream = session.receiveResponse(response);
    Json::Value response_json;

    if (response.getStatus() == Poco::Net::HTTPResponse::HTTP_OK)
    {
        respStream >> response_json;
    } else {
        response_json = Json::Value::null;
    }

    std::string not_resolved = (on_not_resolvable == EMPTY) ? "" : name;

    if (response_json.isNull())
    {
        return not_resolved;
    } else
    {
        Json::Value results = response_json["results"];
        Json::Value first = results[0];
        return first.get("label", not_resolved).asString();
    }
}

/*
void Terminology::requestLabelsBatch(const std::vector<std::string> &names_in,
                               std::vector<std::string> &names_out, const std::string &terminology,
                               const HandleNotResolvable onNotResolvable){
    const int BATCH_SIZE = 10;
    const std::string base_uri = "https://terminologies.gfbio.org/api/terminologies/search";

    //TODO make sure that this context works.
    Poco::Net::Context::Ptr context = new Poco::Net::Context(
            Poco::Net::Context::CLIENT_USE,
            "",
            Poco::Net::Context::VERIFY_RELAXED,
            9,
            true
    );
    context->enableSessionCache(true);

    Poco::URI uri(base_uri);
    std::cout << "names_in size: " << names_in.size() << std::endl;

    //int length = names_in.size() < BATCH_SIZE ? names_in.size() : BATCH_SIZE;
    int batches = names_in.size() / BATCH_SIZE;
    if((names_in.size() % BATCH_SIZE) != 0) {
        batches += 1;
    }
    std::cout << "Batches: " << batches << std::endl;

    Poco::Net::Session::Ptr session_ptr;

    const int names_in_size = names_in.size();

    for(int k = 0; k < batches; k++){

        const int offset = k * BATCH_SIZE;
        std::vector<std::unique_ptr<Poco::Net::HTTPSClientSession>> sessions(BATCH_SIZE);

        //send requests
        for(int i = 0; i < BATCH_SIZE && (k * BATCH_SIZE) + i < names_in_size; i++){
            Poco::URI uri(base_uri);
            uri.addQueryParameter("query", names_in[offset + i]);
            uri.addQueryParameter("terminologies", terminology);

            if(session_ptr.isNull()){
                sessions[i] = make_unique<Poco::Net::HTTPSClientSession>(uri.getHost(), uri.getPort(), context);
                session_ptr = sessions[i]->sslSession();
            } else {
                sessions[i] = make_unique<Poco::Net::HTTPSClientSession>(uri.getHost(), uri.getPort(), context, session_ptr);
            }

            Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
            sessions[i]->sendRequest(request);

        }

        //get responses
        for(int i = 0; i < BATCH_SIZE && (k * BATCH_SIZE) + i < names_in_size; i++){

            Poco::Net::HTTPResponse response;
            std::istream& resp_stream = sessions[i]->receiveResponse(response);
            Json::Value resp;
            resp_stream >> resp;

            if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK || resp.isNull()){
                names_out.push_back((onNotResolvable == EMPTY) ? "" : names_in[offset + i]);
            }
            else {
                Json::Value results = resp["results"];
                Json::Value first = results[0];
                names_out.push_back(first.get("label", (onNotResolvable == EMPTY) ? "" : names_in[offset + i]).asString());
            }
        }


    }
}
 */
/*
void Terminology::requestLabelsAllConcurrent(const std::vector<std::string> &names_in,
                                             std::vector<std::string> &names_out,
                                             const std::string &terminology,
                                             const HandleNotResolvable on_not_resolvable) {

    const std::string base_uri = "https://terminologies.gfbio.org/api/terminologies/search";

    std::vector<std::future<std::string>> futures;

    //Poco::Net::Session::Ptr session_ptr = session.sslSession();

    for(const std::string &name : names_in){


        //futures.push_back(std::move(std::async(std::launch::async, request_label, name, terminology, on_not_resolvable)));

        //names_out.push_back(request_label(s, terminology, on_not_resolvable));

        futures.push_back(std::async(std::launch::async, [&name, &terminology, &base_uri, &on_not_resolvable]() -> std::string {

            Poco::URI uri(base_uri);
            Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort());//, context);
            Poco::URI::QueryParameters q_params;
            q_params.emplace_back("query", name);
            q_params.emplace_back("terminologies", terminology);
            q_params.emplace_back("first_hit", "true");
            uri.setQueryParameters(q_params);

            Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
            Poco::Net::HTTPResponse response;
            session.sendRequest(request);

            std::istream& resp_stream = session.receiveResponse(response);
            Json::Value resp;
            resp_stream >> resp;

            if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK || resp.isNull()){
                return (on_not_resolvable == EMPTY) ? "" : name;
            }
            else {
                Json::Value results = resp["results"];
                Json::Value first = results[0];
                return first.get("label", (on_not_resolvable == EMPTY) ? "" : name).asString();
            }
        }));
    }

    for(auto& f : futures){
        names_out.emplace_back(f.get());
    }

}
*/

std::string Terminology::requestSingleLabel(const std::string &name,
                                            const std::string &terminology,
                                            HandleNotResolvable onNotResolvable)
{
    Poco::URI uri("https://terminologies.gfbio.org/api/terminologies/search");
    uri.addQueryParameter("query", name);
    uri.addQueryParameter("terminologies", terminology);

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

    std::string not_resolved = (onNotResolvable == EMPTY) ? "" : name;

    if (response_json.isNull())
    {
        return not_resolved;
    } else
    {
        Json::Value results = response_json["results"];
        Json::Value first = results[0];
        return first.get("label", not_resolved).asString();
    }
}
