/*
 * zedstoream.h
 *		public declarations for ZedStore
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/zedstoream.h
 */
#ifndef ZEDSTOREAM_H
#define ZEDSTOREAM_H

extern void AtEOXact_zedstore_tuplebuffers(bool isCommit);
extern void AtSubStart_zedstore_tuplebuffers(void);
extern void AtEOSubXact_zedstore_tuplebuffers(bool isCommit);

#endif							/* ZEDSTOREAM_H */
