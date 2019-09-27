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

OBJS = zedstore_btree.o zedstore_tiditem.o zedstore_tidpage.o \
       zedstore_tid.o zedstore_attstream.o zedstore_attpage.o \
       zedstore_compression.o zedstore_simple8b.o zedstoream_handler.o \
       zedstore_meta.o zedstore_undolog.o zedstore_undorec.o \
       zedstore_toast.o zedstore_visibility.o zedstore_inspect.o \
       zedstore_freepagemap.o zedstore_tupslot.o zedstore_wal.o \
       zedstore_tuplebuffer.o

include $(top_srcdir)/src/backend/common.mk
