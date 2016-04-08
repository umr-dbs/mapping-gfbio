#ifndef UTIL_FORMULA_H
#define UTIL_FORMULA_H

#include "util/exceptions.h"

#include <string>
#include <vector>

/*
 * This class is meant to model user-inputted formulas.
 *
 * We may want to concatenate a user-supplied formula into an opencl kernel.
 * Obviously, we must to sanitze it first, making sure it doesn't contain loops, pointer arithmetic etc.
 *
 * So far, this is a stub. It will catch most obvious hacking attempts, but will not catch formulas
 * that are otherwise invalid for proper error reporting.
 */

class Formula {
	public:
		Formula(const std::string &formula);
		~Formula() = default;
		Formula(const Formula &other) = delete;

		void addFunction(size_t arguments, const std::string &sourcename, const std::string &translatedname = "");
		void addCLFunctions();

		void addVariable(const std::string variable, const std::string &translatedname = "");

		std::string parse();

		class parse_error : public std::runtime_error {
			using std::runtime_error::runtime_error;
		};
	private:
		std::string formula;
};


#endif
