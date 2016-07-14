#
# General configuration
# These variables are for system-specific settings and can be overwritten in Makefile.local
# TODO: maybe we should use autotools or cmake or something..
#

# this is the default path when installing libgtest-dev on ubuntu
GTEST_DIR=/usr/src/gtest/

#CPP=g++
CPP=clang++-3.5
LLVM_SYMBOLIZER=/usr/bin/llvm-symbolizer-3.5
ASAN_OPTIONS=
# if you need better stack traces, use something like this. This will slow down the tests by a factor of 5-10!
#ASAN_OPTIONS=fast_unwind_on_malloc=0:malloc_context_size=10
UBSAN_OPTIONS=print_stacktrace=1


USE_OPENCL=true
USE_ABCD=true

DBG_FLAGS=-g
OPT_FLAGS=-O2

MODULES_PATH=..
MODULES_LIST=mapping-gfbio

# to override any of these options, create Makefile.local and set them there
-include Makefile.local

# The testcases are run with full error checking. Currently, the following two tools are used:
# AddressSanitizer, see http://clang.llvm.org/docs/AddressSanitizer.html
# UndefinedBehaviorSanitizer, see http://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html
SANITIZE_FLAGS:=-fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer
# -fno-sanitize-recover=undefined would exit on undefined behaviour, but needs a more recent version of clang
# -fsanitize=integer for warnings on (defined) unsigned overflows

LDFLAGS := -lpthread -pthread -lbz2 -lturbojpeg -lgeos -lboost_date_time
LDFLAGS_CL := -lOpenCL


# these variables will be appended to by the module definitions
ALL_TARGETS := 
OBJS_CORE :=
OBJS_CORE_WITHCL := 
OBJS_CORE_NOCL :=
OBJS_OPERATORS := 
OBJS_SERVICES := 
OBJS_GTEST := 
PKGLIBS_CORE :=
CPPFLAGS := ${DBG_FLAGS} ${OPT_FLAGS} -Wall -Wextra -pedantic-errors -std=c++11 -I.
TEST_QUERIES := $(notdir $(wildcard test/systemtests/queries/*.json))

# Now include the module definitions
include Module.mk
define MODULE_include
  MODULEPATH := $$(MODULES_PATH)/$(1)/
  include $$(MODULEPATH)/Module.mk
  TEST_QUERIES += $$(notdir $$(wildcard $$(MODULEPATH)/test/systemtests/queries/*.json))
endef
$(foreach module,$(MODULES_LIST),$(eval $(call MODULE_include,$(module))))

OBJS_CORE_NOCL := $(OBJS_CORE) $(OBJS_CORE_NOCL)
OBJS_CORE := $(OBJS_CORE) $(OBJS_CORE_WITHCL)

OBJ_OPERATORS_STUBS := $(OBJ_OPERATORS:o/operators/%=o/operators_stub/%)

TEST_LOGS:=$(addprefix o/testresults/,$(TEST_QUERIES:.json=.log))

# before we start generating targets, make sure the first target is "all"
.PHONY: default clean systemtest systemtest_testenvironment systemtest_testcases unittest doc

.DELETE_ON_ERROR:

# disable implicit rules for %.o etc
.SUFFIXES:

default: all


# see https://www.gnu.org/software/make/manual/html_node/Eval-Function.html
# for a similar example
# note that older versions of make do not properly support multi-line variables via 'define'
ALL_NAMES := 
ALL_PKGLIBS := 
ALL_OBJS := 

define TARGET_template
$$(TARGET_$(1)_NAME): $$(TARGET_$(1)_OBJS)
	$$(CPP) $$+ -o $$@ $$(LDFLAGS) $$(TARGET_$(1)_LDFLAGS) $$(shell pkg-config --libs $$(TARGET_$(1)_PKGLIBS))
ALL_NAMES += $$(TARGET_$(1)_NAME)
ALL_PKGLIBS += $$(TARGET_$(1)_PKGLIBS) 
ALL_OBJS += $$(TARGET_$(1)_OBJS)
endef
$(foreach target,$(ALL_TARGETS),$(eval $(call TARGET_template,$(target))))

# sorting is a cheap way to remove duplicates
ALL_OBJS := $(sort $(ALL_OBJS))


# now let's see if all our libraries are present
ALL_PKGLIBS_EXIST := $(shell pkg-config --exists $(ALL_PKGLIBS) >/dev/null 2>&1 ; echo $$?)
ifneq ($(ALL_PKGLIBS_EXIST),0)
  $(shell pkg-config --print-errors $(ALL_PKGLIBS))
  $(error Could not find required library, we need $(ALL_PKGLIBS))
endif

CPPFLAGS += $(shell pkg-config --cflags $(ALL_PKGLIBS))


# TODO: modularize somehow
CL_SOURCES:=$(wildcard operators/*/*.cl) $(wildcard operators/*/*/*.cl)
CL_HEADERS:=$(CL_SOURCES:.cl=.cl.h)


ifneq (${USE_OPENCL},true)
  CPPFLAGS := ${CPPFLAGS} -DMAPPING_NO_OPENCL=1
  LDFLAGS_CL := 
endif



all: $(CL_HEADERS) $(ALL_NAMES)


#
# gtest library
# instructions taken from https://github.com/google/googletest/blob/master/googletest/README.md
#
o/core/gtest-all.o: ${GTEST_DIR}/src/gtest-all.cc
	${CPP} ${CPPFLAGS} -I${GTEST_DIR} -pthread -o $@ -c $<

o/core/libgtest.a: o/core/gtest-all.o
	ar -rv $@ $<


#
# static rules
#

# include the generated dependencies
-include o/*.d o/*/*.d o/*/*/*.d o/*/*/*/*.d

operators/%.cl.h: operators/%.cl
	xxd -i $+ | sed 's/unsigned /static const /g' > $@
	echo $+ | sed -e 's/\.cl$$//' -e 's:/:_:g' -e 's/\(^.*$$\)/static const std::string \1(\1_cl, \1_cl_len);/' >> $@

.PRECIOUS: operators/%.cl.h

# we need to create directories in o/, but directories cannot be dependencies
# so instead we create a file .f in each directory..
o/%/.f:
	mkdir -p $(dir $@)
	touch $@
	
o/.f:
	touch $@

.PRECIOUS: o/%/.f o/.f


# there are variables in the dependencies, we need SECONDEXPANSION for them. 
.SECONDEXPANSION:

o/core/operators_stub/%.o: operators/%.cpp | $${@D}/.f
	${CPP} ${CPPFLAGS} -DMAPPING_OPERATOR_STUBS=1 -MMD -MF ${@:.o=.d} -c $< -o $@

o/core/%.o: %.cpp | $${@D}/.f $(CL_HEADERS)
	${CPP} ${CPPFLAGS} -MMD -MF ${@:.o=.d} -c $< -o $@

# .san.o = compiled with Sanitation
o/core/%.san.o: %.cpp | $${@D}/.f $(CL_HEADERS)
	${CPP} ${CPPFLAGS} ${SANITIZE_FLAGS} -MMD -MF ${@:.o=.d} -c $< -o $@

# note: use /usr/bin/time instead of time, because the latter may be a shell's builtin without the required POSIX options
# The || true at the end is to prevent make from aborting after the first failed test. The actual return code is in the log. 
o/testresults/%.log: test/systemtests/queries/%.json ${TARGET_MANAGER_SAN_NAME} | $${@D}/.f
	export MAPPING_CONFIGURATION=test/systemtests/mapping_test.conf ASAN_SYMBOLIZER_PATH="${LLVM_SYMBOLIZER}" ASAN_OPTIONS="${ASAN_OPTIONS}" UBSAN_OPTIONS="${UBSAN_OPTIONS}" LSAN_OPTIONS="suppressions=test/systemtests/lsan.suppressions" ; /usr/bin/time --format="\nTESTCASE_ELAPSED_TIME: %e\nTESTCASE_RETURN_CODE: %x" ./${TARGET_MANAGER_SAN_NAME} testquery $< >$@ 2>&1 || true

# now we need to create similar rules once for each included module
define MODULE_template
o/$(1)/operators_stub/%.o: $$(MODULES_PATH)/$(1)/operators/%.cpp | $$$${@D}/.f
	$${CPP} $${CPPFLAGS} -I$$(MODULES_PATH)/$(1)/ -DMAPPING_OPERATOR_STUBS=1 -MMD -MF $${@:.o=.d} -c $$< -o $$@

o/$(1)/%.o: $$(MODULES_PATH)/$(1)/%.cpp | $$$${@D}/.f #$$(CL_HEADERS)
	$${CPP} $${CPPFLAGS} -I$$(MODULES_PATH)/$(1)/ -MMD -MF $${@:.o=.d} -c $$< -o $$@

o/$(1)/%.san.o: $$(MODULES_PATH)/$(1)/%.cpp | $$$${@D}/.f #$(CL_HEADERS)
	$${CPP} $${CPPFLAGS} $${SANITIZE_FLAGS} -I$$(MODULES_PATH)/$(1)/ -MMD -MF $${@:.o=.d} -c $$< -o $$@

o/testresults/%.log: $$(MODULES_PATH)/$(1)/test/systemtests/queries/%.json $${TARGET_MANAGER_SAN_NAME} | $$$${@D}/.f
	export MAPPING_CONFIGURATION=test/systemtests/mapping_test.conf ASAN_SYMBOLIZER_PATH="$${LLVM_SYMBOLIZER}" ASAN_OPTIONS="$${ASAN_OPTIONS}" UBSAN_OPTIONS="$${UBSAN_OPTIONS}" LSAN_OPTIONS="suppressions=test/systemtests/lsan.suppressions" ; /usr/bin/time --format="\nTESTCASE_ELAPSED_TIME: %e\nTESTCASE_RETURN_CODE: %x" ./$${TARGET_MANAGER_SAN_NAME} testquery $$< >$$@ 2>&1 || true

endef
$(foreach module,$(MODULES_LIST),$(eval $(call MODULE_template,$(module))))


#
# Targets for data import
#
btw2015_paper_demo_datasource:	${TARGET_MANAGER_NAME}
	/mnt/data/raster_import/btw2015_paper_demo.sh $$(readlink -f ./${TARGET_MANAGER_NAME})

cruts_datasource:	${TARGET_MANAGER_NAME}
	/mnt/data/raster_import/cruts.sh $$(readlink -f ./${TARGET_MANAGER_NAME})

glc2000_global_datasource:	${TARGET_MANAGER_NAME}
	/mnt/data/raster_import/glc2000_global.sh $$(readlink -f ./${TARGET_MANAGER_NAME})

isric_wise_datasource:	${TARGET_MANAGER_NAME}
	/mnt/data/raster_import/isric_wise.sh $$(readlink -f ./${TARGET_MANAGER_NAME})

modis_npp_datasource:
	/mnt/data/raster_import/modis_npp.sh $$(readlink -f ./${TARGET_MANAGER_NAME})

modis_vcf_datasource:
	/mnt/data/raster_import/modis_vcf.sh $$(readlink -f ./${TARGET_MANAGER_NAME})

msat_datasource:	${TARGET_MANAGER_NAME}
	/mnt/data/raster_import/msat.sh $$(readlink -f ./${TARGET_MANAGER_NAME})

srtm_datasource:	${TARGET_MANAGER_NAME}
	/mnt/data/raster_import/srtm.sh $$(readlink -f ./${TARGET_MANAGER_NAME})

worldclim_datasource:	${TARGET_MANAGER_NAME}
	/mnt/data/raster_import/worldclim.sh $$(readlink -f ./${TARGET_MANAGER_NAME})


#
# All the remaining phony targets
#
systemtest: ${TARGET_PARSETESTLOGS_NAME}
	${MAKE} systemtest_testenvironment
	${MAKE} systemtest_testcases
	${TARGET_PARSETESTLOGS_NAME} >test/systemtest_latest.xml
	rm -f ${TEST_LOGS}
	echo "Testcases done";

systemtest_testcases: ${TARGET_MANAGER_NAME} ${TEST_LOGS}
	echo "All logs written"

systemtest_testenvironment: ${TARGET_MANAGER_NAME}
	#rm -f test/systemtests/data/world1.dat test/systemtests/data/world1.db
	rm -f test/systemtests/data/ndvi.dat test/systemtests/data/ndvi.db
	#./${EXE} createsource 4326 test/systemtests/data/world1.tif  >test/systemtests/data/test_world1.json
	#export MAPPING_CONFIGURATION=test/systemtests/mapping_test.conf ; ./${TARGET_MANAGER_NAME} import world1 test/systemtests/data/world1.tif 1 0 0 2000000000 RAW
	export MAPPING_CONFIGURATION=test/systemtests/mapping_test.conf ; bash test/systemtests/data/ndvi/import.sh 

unittest: ${TARGET_GTEST_NAME}
	./${TARGET_GTEST_NAME} --gtest_color=yes --gtest_output="xml:test/unittest_latest.xml" || true

webinstall:
	php htdocs/get_css.php > htdocs/compiled/compiled.release.css
	php htdocs/get_javascript.php > htdocs/compiled/compiled.release.js
	php htdocs/get_templates.php > htdocs/compiled/compiled.release.soy.js

doc:
	./../docs/makeDoc

clean:
	rm -f ${ALL_OBJS} o/gtest-all.o
	rm -f ${ALL_NAMES}
	rm -f $(CL_HEADERS)
	#find o/ -type f -name '*.o' -delete
	find o/ -type f -name '*.d' -delete
	#find o/ -type f -name '*.a' -delete
