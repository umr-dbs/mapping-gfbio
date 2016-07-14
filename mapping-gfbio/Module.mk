
#
# Support for functionality specific to the gfbio project
# https://www.gfbio.org
#
OBJS_CORE += o/mapping-gfbio/util/gfbiodatautil.o

OBJS_OPERATORS += o/mapping-gfbio/operators/abcd_source.o o/mapping-gfbio/operators/gfbio_source.o o/mapping-gfbio/operators/pangaea_source.o

OBJS_SERVICES += o/mapping-gfbio/services/gfbio.o


PKGLIBS_CORE += xerces-c

