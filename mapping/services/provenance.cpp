
#include "services/httpservice.h"
#include "operators/operator.h"

#include <locale>

/*
 * This class serves provenance information of a query.
 *
 * Query pattern: mapping_url/?service=provenance&query={QUERY_STRING}
 */
class ProvenanceService : public HTTPService {
public:
	using HTTPService::HTTPService;
	virtual ~ProvenanceService() = default;

	virtual void run();
};

REGISTER_HTTP_SERVICE(ProvenanceService, "provenance");

void ProvenanceService::run() {
	std::string query = params.get("query");

	auto graph = GenericOperator::fromJSON(query);

	auto provenance = graph->getFullProvenance();

	response.sendContentType("application/json; charset=utf-8");
	response.finishHeaders();
	response << provenance->toJson();
}
