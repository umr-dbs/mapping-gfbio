
#include "util/formula.h"

Formula::Formula(const std::string &formula) : formula(formula) {
}

void Formula::addFunction(size_t arguments, const std::string &sourcename, const std::string &translatedname) {
	// TODO: implement
}
void Formula::addCLFunctions() {
	// https://www.khronos.org/registry/cl/sdk/1.0/docs/man/xhtml/mathFunctions.html

	// Trigonometry
	addFunction(1, "sin");
	addFunction(1, "asin");
	addFunction(1, "cos");
	addFunction(1, "acos");
	addFunction(1, "tan");
	addFunction(1, "atan");

	// integer division
	addFunction(2, "mod", "fmod");
	addFunction(2, "remainder");

	// rounding
	addFunction(1, "ceil");
	addFunction(1, "floor");
	addFunction(1, "round");
	addFunction(1, "trunc");
	addFunction(1, "abs", "fabs");
	addFunction(1, "fract");

	// powers
	addFunction(2, "pow");
	addFunction(1, "sqrt");
	addFunction(1, "exp");
	addFunction(1, "exp2");
	addFunction(1, "exp10");
	addFunction(1, "log");
	addFunction(1, "log2");
	addFunction(1, "log10");
}

void Formula::addVariable(const std::string variable, const std::string &translatedname) {
	// TODO: implement
}


std::string Formula::parse() {
	// TODO: this catches some strings that are dangerous to include in an opencl kernel,
	// but does not actually parse, verify and pretty-print the formula.

	if (formula.find_first_of(";[]{}!#%\"\'\\") != std::string::npos)
		throw parse_error("Formula contains disallowed characters");

	if ((formula.find("/*") != std::string::npos)
	 || (formula.find("//") != std::string::npos)
	 || (formula.find("*/") != std::string::npos)
	 || (formula.find("return") != std::string::npos)) {
		throw parse_error("Formula contains disallowed words");
	}

	return formula;
}
