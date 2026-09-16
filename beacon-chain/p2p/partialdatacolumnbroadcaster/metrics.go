package partialdatacolumnbroadcaster

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	partialMessageUsefulCellsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_partial_message_useful_cells_total",
		Help: "Number of useful cells received via a partial message",
	}, []string{"column_index"})

	partialMessageCellsReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_partial_message_cells_received_total",
		Help: "Number of total cells received via a partial message",
	}, []string{"column_index"})

	partialMessageValidationsDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_partial_message_validations_dropped_total",
		Help: "Number of cell validations dropped because the validator semaphore was saturated",
	}, []string{"column_index"})
)

// RowDAS (EIP-8371) row-topic counters. Labelled by row index rather than column index: on the
// row axis a part is a cell of one blob at some column, so the per-topic quantity is the blob.
var (
	partialMessageRowUsefulCellsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_partial_message_row_useful_cells_total",
		Help: "Number of useful cells received via a partial row message",
	}, []string{"row_index"})

	partialMessageRowCellsReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_partial_message_row_cells_received_total",
		Help: "Number of total cells received via a partial row message",
	}, []string{"row_index"})

	partialMessageRowValidationsDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_partial_message_row_validations_dropped_total",
		Help: "Number of row cell validations dropped because the validator semaphore was saturated",
	}, []string{"row_index"})

	// Recoverable and complete are counted separately because they mean different things for
	// RowDAS: recoverable is when the reconstruction duties can fire, complete is when the row
	// needed no reconstruction at all. The gap between the two counters is the work RowDAS
	// hands to one designated node instead of all of them.
	partialMessageRowsRecoverableTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "beacon_partial_message_rows_recoverable_total",
		Help: "Number of rows that reached the reconstruction threshold via partial messages",
	})

	partialMessageRowsCompleteTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "beacon_partial_message_rows_complete_total",
		Help: "Number of rows that became complete via partial messages",
	})

	// partialMessageRowsServedElsewhereTotal counts the cancellation signal the reconstruction
	// phases were missing: a peer claiming the whole row while we want nothing further from it.
	// Compare against beacon_row_reconstructions_total to see how much duplicate recovery it
	// actually stands down.
	partialMessageRowsServedElsewhereTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "beacon_partial_message_rows_served_elsewhere_total",
		Help: "Number of rows observed to be fully available at a peer while nothing further was wanted locally",
	})

	// partialMessageRowGroupsRefusedTotal counts row groups refused by the equivocation bound,
	// by which bound refused them. Nonzero on a healthy network means a reorg deeper than the
	// per-slot allowance or a proposer equivocating; sustained nonzero means the latter.
	partialMessageRowGroupsRefusedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_partial_message_row_groups_refused_total",
		Help: "Number of row groups refused because the per-slot or per-topic bound was reached",
	}, []string{"bound"})

	// The cross-fill counters are the direct measure of what RowDAS buys: a cell counted
	// under row_to_column is one a column got without any column-topic traffic.
	crossFilledCellsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_partial_message_cross_filled_cells_total",
		Help: "Number of verified cells learned on one DAS axis and filled into the other",
	}, []string{"direction"})

	// crossForwardedColumnsTotal counts column topics pushed into, not cells: one row
	// contributes one cell to each, so this is the fan-out of a single reconstruction.
	crossForwardedColumnsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "beacon_partial_message_cross_forwarded_columns_total",
		Help: "Number of non-custodied column topics a recovered row was pushed into",
	})

	pulledColumnsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "beacon_partial_message_pulled_columns_total",
		Help: "Number of non-custodied column topics a row's missing cells were requested from",
	})
)
