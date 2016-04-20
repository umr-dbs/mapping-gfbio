#include <gtest/gtest.h>
#include "util/formula.h"


static void goodFormula(const std::string &formula) {
	Formula f(formula);
	f.addCLFunctions();
	EXPECT_NO_THROW(f.parse()) << formula;
}

static void badFormula(const std::string &formula) {
	Formula f(formula);
	f.addCLFunctions();
	EXPECT_THROW(f.parse(), Formula::parse_error) << formula;
}

TEST(Formula, good) {
	goodFormula("A*B");
	goodFormula("A+B-C");
	goodFormula("A*sin(pow(B,C))");
}

TEST(Formula, bad) {
	badFormula("return 42");
	badFormula("42;37");
	badFormula("A + \"hello\"");
	badFormula("A + 'a'");
	badFormula("A[7]");
	badFormula("while(1) {}");
	badFormula("while(1) {}");
	badFormula("A % 10"); // must use mod(A, 10)
	badFormula("42 // comment");
	badFormula("42 /* comment */");
}

TEST(Formula, DISABLED_morebad) {
	// These should be caught, but cannot be detected without a full parser.
	badFormula("*(&A + 5)");
	badFormula("42 + exit(5)");
	badFormula("*(0x0042)");
	badFormula("statement(), 42");
}
