/*
 * zedstore_simple8b.c
 *		Simple-8b encoding for zedstore
 *
 * FIXME: This is copy-pasted from src/backend/lib/integerset.c. Some of
 * the things we do here are not relevant for the use in zedstore, or could
 * be optimized. For example, EMPTY_CODEWORD is not used.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_simple8b.h
 */
#include "postgres.h"

#include "access/zedstore_simple8b.h"

/*
 * Decode an array of Simple-8b codewords, known to contain 'num_integers'
 * integers.
 */
void
simple8b_decode_words(uint64 *codewords, int num_codewords,
					  uint64 *dst, int num_integers)
{
	int			total_decoded = 0;

	/* decode all the codewords */
	for (int i = 0; i < num_codewords; i++)
	{
		int			num_decoded;

		num_decoded = simple8b_decode(codewords[i], &dst[total_decoded]);
		total_decoded += num_decoded;
	}
	/*
	 * XXX: This error message is a bit specific, but it matches how this
	 * function is actually used, i.e. to encode TIDs, and the number of integers
	 * comes from the item header.
	 */
	if (total_decoded != num_integers)
		elog(ERROR, "number of TIDs in codewords did not match the item header");
}

/*
 * Simple-8b encoding.
 *
 * The simple-8b algorithm packs between 1 and 240 integers into 64-bit words,
 * called "codewords".  The number of integers packed into a single codeword
 * depends on the integers being packed; small integers are encoded using
 * fewer bits than large integers.  A single codeword can store a single
 * 60-bit integer, or two 30-bit integers, for example.
 *
 * Since we're storing a unique, sorted, set of integers, we actually encode
 * the *differences* between consecutive integers.  That way, clusters of
 * integers that are close to each other are packed efficiently, regardless
 * of their absolute values.
 *
 * In Simple-8b, each codeword consists of a 4-bit selector, which indicates
 * how many integers are encoded in the codeword, and the encoded integers are
 * packed into the remaining 60 bits.  The selector allows for 16 different
 * ways of using the remaining 60 bits, called "modes".  The number of integers
 * packed into a single codeword in each mode is listed in the simple8b_modes
 * table below.  For example, consider the following codeword:
 *
 *      20-bit integer       20-bit integer       20-bit integer
 * 1101 00000000000000010010 01111010000100100000 00000000000000010100
 * ^
 * selector
 *
 * The selector 1101 is 13 in decimal.  From the modes table below, we see
 * that it means that the codeword encodes three 20-bit integers.  In decimal,
 * those integers are 18, 500000 and 20.  Because we encode deltas rather than
 * absolute values, the actual values that they represent are 18, 500018 and
 * 500038.
 *
 * Modes 0 and 1 are a bit special; they encode a run of 240 or 120 zeroes
 * (which means 240 or 120 consecutive integers, since we're encoding the
 * deltas between integers), without using the rest of the codeword bits
 * for anything.
 *
 * Simple-8b cannot encode integers larger than 60 bits.  Values larger than
 * that are always stored in the 'first' field of a leaf item, never in the
 * packed codeword.  If there is a sequence of integers that are more than
 * 2^60 apart, the codeword will go unused on those items.  To represent that,
 * we use a magic EMPTY_CODEWORD codeword value.
 */
static const struct simple8b_mode
{
	uint8		bits_per_int;
	uint8		num_ints;
} simple8b_modes[17] =

{
	{0, 240},					/* mode  0: 240 zeroes */
	{0, 120},					/* mode  1: 120 zeroes */
	{1, 60},					/* mode  2: sixty 1-bit integers */
	{2, 30},					/* mode  3: thirty 2-bit integers */
	{3, 20},					/* mode  4: twenty 3-bit integers */
	{4, 15},					/* mode  5: fifteen 4-bit integers */
	{5, 12},					/* mode  6: twelve 5-bit integers */
	{6, 10},					/* mode  7: ten 6-bit integers */
	{7, 8},						/* mode  8: eight 7-bit integers (four bits
								 * are wasted) */
	{8, 7},						/* mode  9: seven 8-bit integers (four bits
								 * are wasted) */
	{10, 6},					/* mode 10: six 10-bit integers */
	{12, 5},					/* mode 11: five 12-bit integers */
	{15, 4},					/* mode 12: four 15-bit integers */
	{20, 3},					/* mode 13: three 20-bit integers */
	{30, 2},					/* mode 14: two 30-bit integers */
	{60, 1},					/* mode 15: one 60-bit integer */

	{0, 0}						/* sentinel value */
};

/*
 * Maximum number of integers that can be encoded in a single Simple-8b
 * codeword.
 */
#define SIMPLE8B_MAX_VALUES_PER_CODEWORD 240

/*
 * EMPTY_CODEWORD is a special value, used to indicate "no values".
 * It is used if the next value is too large to be encoded with Simple-8b.
 *
 * This value looks like a mode-0 codeword, but we can distinguish it
 * because a regular mode-0 codeword would have zeroes in the unused bits.
 */
#define EMPTY_CODEWORD		UINT64CONST(0x0FFFFFFFFFFFFFFF)

/*
 * Encode a number of integers into a Simple-8b codeword.
 *
 * Returns the encoded codeword, and sets *num_encoded to the number of
 * input integers that were encoded.  That can be zero, if the first delta
 * is too large to be encoded.
 */
uint64
simple8b_encode(const uint64 *ints, int num_ints, int *num_encoded)
{
	int			selector;
	int			nints;
	int			bits;
	uint64		val;
	uint64		codeword;
	int			i;

	/*
	 * Select the "mode" to use for this codeword.
	 *
	 * In each iteration, check if the next value can be represented in the
	 * current mode we're considering.  If it's too large, then step up the
	 * mode to a wider one, and repeat.  If it fits, move on to the next
	 * integer.  Repeat until the codeword is full, given the current mode.
	 *
	 * Note that we don't have any way to represent unused slots in the
	 * codeword, so we require each codeword to be "full".  It is always
	 * possible to produce a full codeword unless the very first delta is too
	 * large to be encoded.  For example, if the first delta is small but the
	 * second is too large to be encoded, we'll end up using the last "mode",
	 * which has nints == 1.
	 */
	selector = 0;
	nints = simple8b_modes[0].num_ints;
	bits = simple8b_modes[0].bits_per_int;
	val = ints[0];
	i = 0;						/* number of deltas we have accepted */
	for (;;)
	{
		if (val >= (UINT64CONST(1) << bits))
		{
			/* too large, step up to next mode */
			selector++;
			nints = simple8b_modes[selector].num_ints;
			bits = simple8b_modes[selector].bits_per_int;
			/* we might already have accepted enough deltas for this mode */
			if (i >= nints)
				break;
		}
		else
		{
			/* accept this delta; then done if codeword is full */
			i++;
			if (i >= nints)
				break;
			/* examine next delta */
			if (i < num_ints)
				val = ints[i];
			else
			{
				/*
				 * Reached end of input. Pretend that the next integer is a
				 * value that's too large to represent in Simple-8b, so that
				 * we fall out.
				 */
				val = PG_UINT64_MAX;
			}
		}
	}

	if (nints == 0)
	{
		/*
		 * The first delta is too large to be encoded with Simple-8b.
		 *
		 * If there is at least one not-too-large integer in the input, we
		 * will encode it using mode 15 (or a more compact mode).  Hence, we
		 * can only get here if the *first* delta is >= 2^60.
		 */
		Assert(i == 0);
		*num_encoded = 0;
		return EMPTY_CODEWORD;
	}

	/*
	 * Encode the integers using the selected mode.  Note that we shift them
	 * into the codeword in reverse order, so that they will come out in the
	 * correct order in the decoder.
	 */
	codeword = 0;
	if (bits > 0)
	{
		for (i = nints - 1; i > 0; i--)
		{
			val = ints[i];
			codeword |= val;
			codeword <<= bits;
		}
		val = ints[0];
		codeword |= val;
	}

	/* add selector to the codeword, and return */
	codeword |= (uint64) selector << 60;

	*num_encoded = nints;
	return codeword;
}

/*
 * Encode a number of same integers into a Simple-8b codeword.
 *
 * This is a special version of simple8b_encode, where the first input
 * integer is 'firstint', followed by a number of 'secondint'. This is
 * equivalent to calling simple8b_encode() with an input array:
 *
 * ints[0]: firstint
 * ints[1]: secondint
 * ints[2]: secondint
 * ...
 * ints[num_ints - 1]: secondint
 *
 *
 * We need that when doing a multi-insert, and it seems nice to have a
 * specialized version for that, for speed, but also to keep the calling
 * code simpler, so that it doesn't need to construct an input array.
 *
 * TODO: This is just copy-pasted from simple8b_encode, but since we know
 * what the input is, we could probably optimize this further.
 */
uint64
simple8b_encode_consecutive(const uint64 firstint, const uint64 secondint, int num_ints,
							int *num_encoded)
{
	int			selector;
	int			nints;
	int			bits;
	uint64		val;
	uint64		codeword;
	int			i;

	/*
	 * Select the "mode" to use for this codeword.
	 *
	 * In each iteration, check if the next value can be represented in the
	 * current mode we're considering.  If it's too large, then step up the
	 * mode to a wider one, and repeat.  If it fits, move on to the next
	 * integer.  Repeat until the codeword is full, given the current mode.
	 *
	 * Note that we don't have any way to represent unused slots in the
	 * codeword, so we require each codeword to be "full".  It is always
	 * possible to produce a full codeword unless the very first delta is too
	 * large to be encoded.  For example, if the first delta is small but the
	 * second is too large to be encoded, we'll end up using the last "mode",
	 * which has nints == 1.
	 */
	selector = 0;
	nints = simple8b_modes[0].num_ints;
	bits = simple8b_modes[0].bits_per_int;
	val = firstint;
	i = 0;						/* number of deltas we have accepted */
	for (;;)
	{
		if (val >= (UINT64CONST(1) << bits))
		{
			/* too large, step up to next mode */
			selector++;
			nints = simple8b_modes[selector].num_ints;
			bits = simple8b_modes[selector].bits_per_int;
			/* we might already have accepted enough deltas for this mode */
			if (i >= nints)
				break;
		}
		else
		{
			/* accept this delta; then done if codeword is full */
			i++;
			if (i >= nints)
				break;
			/* examine next delta */
			if (i < num_ints)
				val = secondint;
			else
			{
				/*
				 * Reached end of input. Pretend that the next integer is a
				 * value that's too large to represent in Simple-8b, so that
				 * we fall out.
				 */
				val = PG_UINT64_MAX;
			}
		}
	}

	if (nints == 0)
	{
		/*
		 * The first delta is too large to be encoded with Simple-8b.
		 *
		 * If there is at least one not-too-large integer in the input, we
		 * will encode it using mode 15 (or a more compact mode).  Hence, we
		 * can only get here if the *first* delta is >= 2^60.
		 */
		Assert(i == 0);
		*num_encoded = 0;
		return EMPTY_CODEWORD;
	}

	/*
	 * Encode the integers using the selected mode.  Note that we shift them
	 * into the codeword in reverse order, so that they will come out in the
	 * correct order in the decoder.
	 */
	codeword = 0;
	if (bits > 0)
	{
		for (i = nints - 1; i > 0; i--)
		{
			val = secondint;
			codeword |= val;
			codeword <<= bits;
		}
		val = firstint;
		codeword |= val;
	}

	/* add selector to the codeword, and return */
	codeword |= (uint64) selector << 60;

	*num_encoded = nints;
	return codeword;
}

/*
 * Decode a codeword into an array of integers.
 * Returns the number of integers decoded.
 */
int
simple8b_decode(uint64 codeword, uint64 *decoded)
{
	int			selector = (codeword >> 60);
	int			nints = simple8b_modes[selector].num_ints;
	int			bits = simple8b_modes[selector].bits_per_int;
	uint64		mask = (UINT64CONST(1) << bits) - 1;

	if (codeword == EMPTY_CODEWORD)
		return 0;

	for (int i = 0; i < nints; i++)
	{
		uint64		val = codeword & mask;

		decoded[i] = val;
		codeword >>= bits;
	}

	return nints;
}
