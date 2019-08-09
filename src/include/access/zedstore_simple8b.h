/*
 * zedstore_simple8b.h
 *		XXX
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/zedstore_simple8b.h
 */
#ifndef ZEDSTORE_SIMPLE8B_H
#define ZEDSTORE_SIMPLE8B_H

extern uint64 simple8b_encode(const uint64 *ints, int num_ints, int *num_encoded);
extern uint64 simple8b_encode_consecutive(const uint64 firstint, const uint64 secondint, int num_ints,
										  int *num_encoded);
extern int simple8b_decode(uint64 codeword, uint64 *decoded);

#endif							/* ZEDSTORE_SIMPLE8B_H */
