#ifndef OPERATORS_PROVENANCE_H
#define OPERATORS_PROVENANCE_H

#include <string>
#include <vector>

/**
 * Provenance
 *
 * While the Operator graph contains all information required to repeat a specific calculations,
 * it is not always obvious which data was used in the workflow. Operators are free to load
 * any data they deem required for the satisfaction of the query.
 *
 * For example, a rasterdb_source operator with parameter sourcename=foo would be expected
 * to use only data from the raster database named foo. But this is only an expectation from
 * the user, and would need to be verified by looking at the source code of the operator.
 *
 * Instead, we require each operator to provide Provenance information. The rasterdb_source
 * would then report that yes, it is indeed only using data from the rasterdb named foo, along
 * with licence and citation where available.
 */
class Provenance {
	public:
	Provenance() = default;
	Provenance(const std::string &citation, const std::string &license, const std::string &uri, const std::string &local_identifier);

	/*
	 * A citation describing name and creator(s) of the data. This may contain names of author(s)
	 * or organisations, it may cite a paper where the data was published etc.
	 *
	 * Examples:
	 *  FooDataset, (C) 1942 by the Foo Foundation
	 *  BarDataset, "A new approach towards Bar", ACM Bar, 1942, pg. 142-148
	 */
	std::string citation;
	/*
	 * A license. This is NOT the full license text, but just the name of the licence.
	 * Examples: proprietary, public domain, CC-SA-NC, MIT, ...
	 */
	std::string license;
	/*
	 * A global identifier for the dataset. Preferably an URL to a webpage about the data,
	 * but any URI is acceptable.
	 */
	std::string uri;
	/*
	 * A local identifier, used within the mapping system for permission management.
	 * Operators should prefix these identifiers with data.<operator_name>.
	 *
	 * Example:
	 *  data.rasterdb_source.foodataset
	 *  data.postgres_source.foodatabase.bartable
	 */
	std::string local_identifier;
};

/**
 * ProvenanceCollection
 *
 * A workflow will usually contain more than one dataset. The idea is to have one Provenance
 * object per dataset, and collect all these objects in a ProvenanceCollection.
 */
class ProvenanceCollection {
	public:
		/*
		 * Add provenance information for another data set.
		 */
		void add(const Provenance &provenance);
		/*
		 * Serializes the collection into Json format
		 */
		std::string toJson();
		/*
		 * Returns a list of all local identifiers used in this Collection, without duplicates.
		 */
		std::vector<std::string> getLocalIdentifiers();
	private:
		std::vector<Provenance> items;
};



#endif
