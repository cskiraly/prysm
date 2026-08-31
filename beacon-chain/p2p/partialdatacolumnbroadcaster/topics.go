package partialdatacolumnbroadcaster

import (
	"strconv"
	"strings"

	p2ptypes "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/types"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/runtime/version"
	"github.com/pkg/errors"
)

// The subnet-indexed parts of the gossip topic names this broadcaster serves. They are
// duplicated here rather than imported from beacon-chain/p2p, which imports this package;
// beacon-chain/p2p carries a test asserting the two agree.
const (
	dataColumnSidecarPrefix = "data_column_sidecar_"

	// DataRowPrefix is the RowDAS (EIP-8371) row topic prefix.
	DataRowPrefix = "data_row_"
)

// numberOfColumns is the number of parts in a row: a row is an extended blob, so it has one
// cell per column, whatever the block's blob count.
const numberOfColumns = uint64(fieldparams.NumberOfColumns)

// topicKind says which DAS axis a topic carries. The broadcaster serves both from one
// gossipsub partial-messages extension, because the extension is instantiated once per host
// with a single peer-state type -- and because cells learned on one axis are wanted on the
// other, which is cheapest to arrange inside a single event loop.
type topicKind uint8

const (
	topicKindUnknown topicKind = iota
	topicKindColumn
	topicKindRow
)

func (k topicKind) String() string {
	switch k {
	case topicKindColumn:
		return "column"
	case topicKindRow:
		return "row"
	default:
		return "unknown"
	}
}

var (
	errUnknownTopicKind = errors.New("topic is neither a data column nor a data row topic")
	errAmbiguousTopic   = errors.New("topic contains both a data column and a data row prefix")
)

// classifyTopic says whether a topic is a column or a row topic and returns its subnet index,
// bounds-checked for that axis. The topic string is peer-controlled, so an unrecognised or
// out-of-range topic is an error rather than a default.
func classifyTopic(topic string) (topicKind, uint64, error) {
	columnAt := strings.Index(topic, dataColumnSidecarPrefix)
	rowAt := strings.Index(topic, DataRowPrefix)

	switch {
	case columnAt >= 0 && rowAt >= 0:
		return topicKindUnknown, 0, errAmbiguousTopic
	case columnAt >= 0:
		columnIndex, err := parseSubnetSuffix(topic, columnAt+len(dataColumnSidecarPrefix))
		if err != nil {
			return topicKindUnknown, 0, errors.Wrap(err, "column index")
		}
		if columnIndex >= fieldparams.NumberOfColumns {
			return topicKindUnknown, 0, errors.Errorf("column index %d out of range", columnIndex)
		}
		return topicKindColumn, columnIndex, nil
	case rowAt >= 0:
		rowSubnet, err := parseSubnetSuffix(topic, rowAt+len(DataRowPrefix))
		if err != nil {
			return topicKindUnknown, 0, errors.Wrap(err, "row subnet")
		}
		rowSubnetCount := params.BeaconConfig().RowSubnetCount
		if rowSubnetCount == 0 || rowSubnet >= rowSubnetCount {
			return topicKindUnknown, 0, errors.Errorf("row subnet %d out of range for ROW_SUBNET_COUNT %d", rowSubnet, rowSubnetCount)
		}
		return topicKindRow, rowSubnet, nil
	default:
		return topicKindUnknown, 0, errUnknownTopicKind
	}
}

// parseSubnetSuffix reads the decimal subnet index that starts at from and ends at the next
// path separator or the end of the string.
func parseSubnetSuffix(topic string, from int) (uint64, error) {
	suffix := topic[from:]
	if end := strings.Index(suffix, "/"); end != -1 {
		suffix = suffix[:end]
	}

	return strconv.ParseUint(suffix, 10, 64)
}

// extractColumnIndexFromTopic returns the column index of a data column topic.
//
// Note that it reads the topic's *subnet* index and treats it as a column index, which holds
// only while DATA_COLUMN_SIDECAR_SUBNET_COUNT equals NUMBER_OF_COLUMNS. That assumption is
// pre-existing and unchanged here.
func extractColumnIndexFromTopic(topic string) (uint64, error) {
	kind, columnIndex, err := classifyTopic(topic)
	if err != nil {
		return 0, err
	}
	if kind != topicKindColumn {
		return 0, errors.Errorf("topic %q is a %s topic, not a column topic", topic, kind)
	}

	return columnIndex, nil
}

func topicForkIsGloas(topic string) (isGloas bool, err error) {
	digest, err := p2ptypes.ExtractGossipDigest(topic)
	if err != nil {
		return false, errors.Wrap(err, "ExtractGossipDigest")
	}
	_, epoch, err := params.ForkDataFromDigest(digest)
	if err != nil {
		return false, errors.Wrap(err, "ForkDataFromDigest")
	}
	return params.GetNetworkScheduleEntry(epoch).VersionEnum >= version.Gloas, nil
}
