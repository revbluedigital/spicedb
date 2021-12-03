package common

import (
	"context"
	"sort"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Changes represents a set of tuple mutations that are kept self-consistent
// across one or more transaction revisions.
type Changes map[uint64]*changeRecord

type changeRecord struct {
	tupleTouches map[string]*v0.RelationTuple
	tupleDeletes map[string]*v0.RelationTuple
}

// AddChange adds a specific change to the complete list of tracked changes
func (ch Changes) AddChange(
	ctx context.Context,
	revision uint64,
	tpl *v0.RelationTuple,
	op v0.RelationTupleUpdate_Operation,
) {
	revisionChanges, ok := ch[revision]
	if !ok {
		revisionChanges = &changeRecord{
			tupleTouches: make(map[string]*v0.RelationTuple),
			tupleDeletes: make(map[string]*v0.RelationTuple),
		}
		ch[revision] = revisionChanges
	}

	tplKey := tuple.String(tpl)

	switch op {
	case v0.RelationTupleUpdate_TOUCH:
		// If there was a delete for the same tuple at the same revision, drop it
		delete(revisionChanges.tupleDeletes, tplKey)

		revisionChanges.tupleTouches[tplKey] = tpl

	case v0.RelationTupleUpdate_DELETE:
		_, alreadyTouched := revisionChanges.tupleTouches[tplKey]
		if !alreadyTouched {
			revisionChanges.tupleDeletes[tplKey] = tpl
		}
	default:
		log.Ctx(ctx).Fatal().Stringer("operation", op).Msg("unknown change operation")
	}
}

func (ch Changes) RevisionChanges() (changes []*datastore.RevisionChanges) {
	revisionsWithChanges := make([]uint64, 0, len(ch))
	for k := range ch {
		revisionsWithChanges = append(revisionsWithChanges, k)
	}
	sort.Slice(revisionsWithChanges, func(i int, j int) bool {
		return revisionsWithChanges[i] < revisionsWithChanges[j]
	})

	for _, rev := range revisionsWithChanges {
		revisionChange := &datastore.RevisionChanges{
			Revision: revisionFromTransaction(rev),
		}

		revisionChangeRecord := ch[rev]
		for _, tpl := range revisionChangeRecord.tupleTouches {
			revisionChange.Changes = append(revisionChange.Changes, &v0.RelationTupleUpdate{
				Operation: v0.RelationTupleUpdate_TOUCH,
				Tuple:     tpl,
			})
		}
		for _, tpl := range revisionChangeRecord.tupleDeletes {
			revisionChange.Changes = append(revisionChange.Changes, &v0.RelationTupleUpdate{
				Operation: v0.RelationTupleUpdate_DELETE,
				Tuple:     tpl,
			})
		}
		changes = append(changes, revisionChange)
	}

	return
}

func revisionFromTransaction(txID uint64) datastore.Revision {
	return decimal.NewFromInt(int64(txID))
}
