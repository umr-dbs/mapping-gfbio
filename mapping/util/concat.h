#ifndef UTIL_CONCAT_H
#define UTIL_CONCAT_H

#include <sstream>

template<typename Head>
void _internal_concat(std::ostringstream &ss, const Head &head) {
	ss << head;
}

template<typename Head, typename... Tail>
void _internal_concat(std::ostringstream &ss, const Head &head, const Tail &... tail) {
	ss << head;
	_internal_concat(ss, tail...);
}



template<typename... Params>
std::string concat(const Params &... params) {
	std::ostringstream ss;
	_internal_concat(ss, params...);
	return ss.str();
}


#endif
