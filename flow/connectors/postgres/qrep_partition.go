package connpostgres

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type PartitionParams struct {
	tx              pgx.Tx
	lastRangeEnd    any
	logger          log.Logger
	watermarkTable  string
	watermarkColumn string
	numPartitions   int64
}

type PartitioningFunc func(context.Context, PartitionParams) ([]*protos.QRepPartition, error)

// NTileBucketPartitioningFunc is a table partition implementation that divides rows into approximately
// equal-sized partitions based on row count. It uses the NTILE window function to assign rows
// to buckets and ensures more balanced row distribution across partitions.
func NTileBucketPartitioningFunc(ctx context.Context, pp PartitionParams) ([]*protos.QRepPartition, error) {
	const partitionsQueryTemplate = `SELECT bucket, MIN(%[2]s) AS start, MAX(%[2]s) AS end
		FROM (
			SELECT NTILE(%[1]d) OVER (ORDER BY %[2]s) AS bucket, %[2]s FROM %[3]s%[4]s
		) subquery
		GROUP BY bucket
		ORDER BY start`

	var partitionsQuery string
	var rows pgx.Rows
	var err error
	if pp.lastRangeEnd != nil {
		partitionsQuery = fmt.Sprintf(
			partitionsQueryTemplate,
			pp.numPartitions,
			pp.watermarkColumn,
			pp.watermarkTable,
			fmt.Sprintf(" WHERE %s > $1", pp.watermarkColumn),
		)
		pp.logger.Info("[row_based_next] partitions query", slog.String("query", partitionsQuery))
		rows, err = pp.tx.Query(ctx, partitionsQuery, pp.lastRangeEnd)
	} else {
		partitionsQuery = fmt.Sprintf(
			partitionsQueryTemplate,
			pp.numPartitions,
			pp.watermarkColumn,
			pp.watermarkTable,
			"",
		)
		pp.logger.Info("[row_based] partitions query", slog.String("query", partitionsQuery))
		rows, err = pp.tx.Query(ctx, partitionsQuery)
	}
	if err != nil {
		return nil, shared.LogError(pp.logger, fmt.Errorf("failed to query for partitions: %w", err))
	}
	defer rows.Close()

	partitionHelper := utils.NewPartitionHelper(pp.logger)
	for rows.Next() {
		var bucket pgtype.Int8
		var start, end any
		if err := rows.Scan(&bucket, &start, &end); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		if err := partitionHelper.AddPartition(start, end); err != nil {
			return nil, fmt.Errorf("failed to add partition: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read rows: %w", err)
	}
	return partitionHelper.GetPartitions(), nil
}

// MinMaxRangePartitioningFunc is a table partition strategy where partitions are created
// by uniformly splitting the min/max value range. Note that partition boundaries are uniform,
// but actual row distribution may be skewed due to non-uniform data distribution, gaps in the
// value range, or deleted rows.
func MinMaxRangePartitioningFunc(ctx context.Context, pp PartitionParams) ([]*protos.QRepPartition, error) {
	var row pgx.Row
	if pp.lastRangeEnd != nil {
		minmaxQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s WHERE %s > $1",
			pp.watermarkColumn, pp.watermarkColumn, pp.watermarkTable, pp.watermarkColumn)
		row = pp.tx.QueryRow(ctx, minmaxQuery, pp.lastRangeEnd)
	} else {
		minmaxQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s",
			pp.watermarkColumn, pp.watermarkColumn, pp.watermarkTable)
		row = pp.tx.QueryRow(ctx, minmaxQuery)
	}

	var start, end any
	if err := row.Scan(&start, &end); err != nil {
		return nil, err
	}

	partitionHelper := utils.NewPartitionHelper(pp.logger)
	if err := partitionHelper.AddPartitionsWithRange(start, end, pp.numPartitions); err != nil {
		return nil, fmt.Errorf("failed to add partitions: %w", err)
	}
	return partitionHelper.GetPartitions(), nil
}

// CTIDBlockPartitioningFunc is a table partition strategy where partitions are created by dividing table
// blocks uniformly. Note that partition boundaries (block ranges) are uniform, but actual row distribution
// may be skewed due to table bloat, deleted tuples, or uneven data distribution across blocks.
func CTIDBlockPartitioningFunc(ctx context.Context, pp PartitionParams) ([]*protos.QRepPartition, error) {
	if pp.numPartitions <= 1 {
		return nil, errors.New("expect numPartitions to be greater than 1")
	}

	blocksQuery := "SELECT (pg_relation_size(to_regclass($1)) / current_setting('block_size')::int)::bigint"
	var totalBlocks pgtype.Int8
	if err := pp.tx.QueryRow(ctx, blocksQuery, pp.watermarkTable).Scan(&totalBlocks); err != nil {
		return nil, fmt.Errorf("failed to get relation blocks: %w", err)
	}
	if !totalBlocks.Valid || totalBlocks.Int64 <= 0 {
		return nil, fmt.Errorf("failed to get valid block count: total blocks: %d, valid: %t", totalBlocks.Int64, totalBlocks.Valid)
	}

	tidCmp := func(a pgtype.TID, b pgtype.TID) int {
		if blockCmp := cmp.Compare(a.BlockNumber, b.BlockNumber); blockCmp != 0 {
			return blockCmp
		}
		return cmp.Compare(a.OffsetNumber, b.OffsetNumber)
	}

	tidInc := func(t pgtype.TID) pgtype.TID {
		if t.OffsetNumber < math.MaxUint16 {
			return pgtype.TID{BlockNumber: t.BlockNumber, OffsetNumber: t.OffsetNumber + 1, Valid: true}
		}
		return pgtype.TID{BlockNumber: t.BlockNumber + 1, OffsetNumber: 0, Valid: true}
	}

	tidRangeForPartition := func(partitionIndex int64) (pgtype.TID, pgtype.TID, bool) {
		blockStart := uint32((partitionIndex * totalBlocks.Int64) / pp.numPartitions)
		nextPartitionBlockStart := uint32(((partitionIndex + 1) * totalBlocks.Int64) / pp.numPartitions)
		if nextPartitionBlockStart <= blockStart {
			return pgtype.TID{}, pgtype.TID{}, false
		}
		tidStartInclusive := pgtype.TID{BlockNumber: blockStart, OffsetNumber: 0, Valid: true}
		tidEndInclusive := pgtype.TID{BlockNumber: nextPartitionBlockStart - 1, OffsetNumber: math.MaxUint16, Valid: true}
		return tidStartInclusive, tidEndInclusive, true
	}

	var resumeFrom pgtype.TID
	if pp.lastRangeEnd != nil {
		if lastTID, ok := pp.lastRangeEnd.(pgtype.TID); ok {
			resumeFrom = tidInc(lastTID)
		} else {
			pp.logger.Warn("Ignoring resume offset because it's not TidRange")
		}
	}

	partitionHelper := utils.NewPartitionHelper(pp.logger)
	for i := range pp.numPartitions {
		start, end, valid := tidRangeForPartition(i)
		if !valid {
			continue
		}
		if resumeFrom.Valid {
			if tidCmp(end, resumeFrom) < 0 {
				continue
			}
			if tidCmp(start, resumeFrom) < 0 {
				start = resumeFrom
			}
		}
		if err := partitionHelper.AddPartition(
			pgtype.TID{BlockNumber: start.BlockNumber, OffsetNumber: start.OffsetNumber, Valid: true},
			pgtype.TID{BlockNumber: end.BlockNumber, OffsetNumber: end.OffsetNumber, Valid: true},
		); err != nil {
			return nil, fmt.Errorf("failed to add TID partition: %w", err)
		}
	}
	return partitionHelper.GetPartitions(), nil
}

// ComputeNumPartitions computes the number of partitions given desired number of rows
// per partition, with automatic adjustment to respect the maximum partition limit.
// TODO: use estimated row count instead to speed up query execution on large tables
func ComputeNumPartitions(ctx context.Context, pp PartitionParams, numRowsPerPartition int64) (int64, error) {
	var row pgx.Row
	if pp.lastRangeEnd != nil {
		countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s > $1`, pp.watermarkTable, pp.watermarkColumn)
		row = pp.tx.QueryRow(ctx, countQuery, pp.lastRangeEnd)
	} else {
		countQuery := "SELECT COUNT(*) FROM " + pp.watermarkTable
		row = pp.tx.QueryRow(ctx, countQuery)
	}

	var totalRows pgtype.Int8
	if err := row.Scan(&totalRows); err != nil {
		return 0, fmt.Errorf("failed to query for total rows: %w", err)
	}

	if totalRows.Int64 == 0 {
		pp.logger.Warn("no records to replicate, returning")
		return 0, nil
	}

	adjustedPartitions := shared.AdjustNumPartitions(totalRows.Int64, numRowsPerPartition)
	pp.logger.Info("[postgres] partition adjustment details",
		slog.Int64("totalRows", totalRows.Int64),
		slog.Int64("desiredNumRowsPerPartition", numRowsPerPartition),
		slog.Int64("adjustedNumPartitions", adjustedPartitions.AdjustedNumPartitions),
		slog.Int64("adjustedNumRowsPerPartition", adjustedPartitions.AdjustedNumRowsPerPartition))
	numPartitions := adjustedPartitions.AdjustedNumPartitions

	return numPartitions, nil
}
