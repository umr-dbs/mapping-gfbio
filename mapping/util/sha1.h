#ifndef UTIL_SHA1_H
#define UTIL_SHA1_H

#include <string>
#include <memory>

// We don't want to include boost headers globally..
namespace boost {
	namespace uuids {
		namespace detail {
			class sha1;
		}
	}
}

/*
 * This is a simple wrapper around an SHA1 implementation.
 * We're currently using an implementation included in boost, but may change this later.
 */
class SHA1 {
	public:
		class SHA1Value {
			public:
				~SHA1Value();
				std::string asHex();
			private:
				SHA1Value(char *value);
				char value[20];
				friend class SHA1;
		};
		SHA1();
		~SHA1();
		void addBytes(const char *data, size_t size);
		void addBytes(const std::string &str) { addBytes(str.c_str(), str.length()); }
		SHA1Value digest();
	private:
		std::unique_ptr<boost::uuids::detail::sha1> s;
};

#endif // UTIL_SHA1_H
