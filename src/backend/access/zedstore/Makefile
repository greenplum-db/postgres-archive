#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/zedstore
#
# IDENTIFICATION
#    src/backend/access/zedstore/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/zedstore
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = zedstore_btree.o zedstore_compression.o zedstoream_handler.o \
       zedstore_meta.o zedstore_undo.o zedstore_toast.o zedstore_visibility.o \
       zedstore_inspect.o zedstore_freepagemap.o zedstore_utils.o \
       zedstore_tupslot.o

include $(top_srcdir)/src/backend/common.mk
