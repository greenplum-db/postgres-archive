/*-------------------------------------------------------------------------
 *
 * zedstore_utils.c
 *	  ZedStore utility functions
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_freepagemap.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/zedstore_internal.h"
#include "miscadmin.h"

/*
 * Allocate a new zs_split_stack struct.
 */
zs_split_stack *
zs_new_split_stack_entry(Buffer buf, Page page)
{
	zs_split_stack *stack;

	stack = palloc(sizeof(zs_split_stack));
	stack->next = NULL;
	stack->buf = buf;
	stack->page = page;
	stack->recycle = false;		/* caller can change this */

	return stack;
}

/*
 * Apply all the changes represented by a list of zs_split_stack
 * entries.
 */
void
zs_apply_split_changes(Relation rel, zs_split_stack *stack)
{
	START_CRIT_SECTION();

	while (stack)
	{
		zs_split_stack *next;

		PageRestoreTempPage(stack->page, BufferGetPage(stack->buf));
		MarkBufferDirty(stack->buf);

		/* add this page to the Free Page Map for recycling */
		if (stack->recycle)
			zspage_delete_page(rel, stack->buf);

		UnlockReleaseBuffer(stack->buf);

		next = stack->next;
		pfree(stack);
		stack = next;
	}

	/* TODO: WAL-log all the changes  */

	END_CRIT_SECTION();
}
