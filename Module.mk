
#
# Support for functionality specific to the gfbio project
# https://www.gfbio.org
#
OBJS_CORE += o/mapping-gfbio/util/gfbiodatautil.o o/mapping-gfbio/util/pangaeaapi.o

OBJS_OPERATORS += o/mapping-gfbio/operators/abcd_source.o o/mapping-gfbio/operators/gfbio_source.o o/mapping-gfbio/operators/pangaea_source.o

OBJS_SERVICES += o/mapping-gfbio/services/gfbio.o o/mapping-gfbio/portal/basketapi.o


LDFLAGS += -lpugixml
