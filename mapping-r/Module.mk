
#
# Support for a server process which executes R scripts and an operator which can use the service
#

# Set these variables in Makefile.config in the core module. If not set, these defaults will be used.
RINSIDE_PATH ?= /usr/local/lib/R/site-library/***REMOVED***/
RCPP_PATH ?= /usr/local/lib/R/site-library/***REMOVED***/


CPPFLAGS += -I$(RINSIDE_PATH)include/ -I$(RCPP_PATH)include/ -I/usr/share/R/include/


OBJS_OPERATORS += o/mapping-r/operators/r_script.o


ALL_TARGETS += RSERVER
TARGET_RSERVER_NAME := r_server
TARGET_RSERVER_OBJS = ${OBJS_CORE_NOCL} o/mapping-r/rserver/rserver.o o/core/util/server_nonblocking.o
TARGET_RSERVER_PKGLIBS := ${PKGLIBS_CORE}
TARGET_RSERVER_LDFLAGS := ${LDFLAGS} -L/usr/lib/R/lib -lR -L$(RINSIDE_PATH)lib -l***REMOVED*** -Wl,-rpath,$(RINSIDE_PATH)lib

