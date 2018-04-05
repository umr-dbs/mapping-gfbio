
#include "util/terminology.h"
#include <gtest/gtest.h>

TEST(Terminology, requestLabel){
    std::string first = "plum";
    std::string second = "honey bee";
    std::string term1 = "NCBITAXON";
    std::string term2 = "PESI";
    EXPECT_EQ(Terminology::resolveSingle(first, term1, "label", "exact", true, HandleNotResolvable::EMPTY), "Prunus domestica");
    EXPECT_EQ(Terminology::resolveSingle(second, term2, "label", "exact", true, HandleNotResolvable::EMPTY), "Apis mellifera Linnaeus, 1758");
}

TEST(Terminology, multiplerequests){
    std::string terminology("NCBITAXON");
    std::vector<std::string> names_in;
    const int num = 20;
    for(int i = 0; i < num; i++){
        names_in.emplace_back("plum");
        names_in.emplace_back("honey bee");
    }
    names_in.emplace_back("dose");

    std::vector<std::string> names_out = Terminology::resolveMultiple(names_in, terminology, "label", "exact", true, HandleNotResolvable::EMPTY);

    EXPECT_EQ(names_out.size(), num*2+1);
    for(int i = 0; i < num; i++) {
        EXPECT_EQ(names_out[i * 2], "Prunus domestica");
        EXPECT_EQ(names_out[i * 2 + 1], "Apis mellifera");
    }
    EXPECT_EQ(names_out[num*2], "");
}
