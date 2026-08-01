package config

import (
	"errors"
	"math"
)

const (
	PerFlowBytes      uint64 = 1_730_560
	PerSessionBytes   uint64 = 1_196_032
	ObserveBytes      uint64 = 3_051_520
	ClientGlobalBytes uint64 = 16 << 20
	ServerGlobalBytes uint64 = 64 << 20

	DefaultClientRequiredBytes uint64 = 3_642_921_984
	DefaultServerRequiredBytes uint64 = 19_171_021_824
)

var ErrBudget = errors.New("config: memory budget")

func ClientRequiredBytes(limits ClientLimits) (uint64, error) {
	terms := []budgetTerm{
		{limits.Flows, PerFlowBytes},
		{limits.Sessions, PerSessionBytes},
		{limits.AuthInProgress, 4096},
		{1, 1024},
		{limits.SOCKSConnections, 1024},
		{1, ObserveBytes},
		{1, ClientGlobalBytes},
	}
	return sumBudget(terms)
}

func ServerRequiredBytes(limits ServerLimits, principals uint64) (uint64, error) {
	terms := []budgetTerm{
		{limits.Flows, PerFlowBytes},
		{limits.Sessions, PerSessionBytes},
		{limits.AuthInProgress, 4096},
		{principals, 1024},
		{limits.Tombstones, 512},
		{limits.RateLimitKeys, 256},
		{limits.TargetDials, 4096},
		{1, ObserveBytes},
		{1, ServerGlobalBytes},
	}
	return sumBudget(terms)
}

type budgetTerm struct {
	count uint64
	size  uint64
}

func sumBudget(terms []budgetTerm) (uint64, error) {
	var total uint64
	for _, term := range terms {
		if term.count != 0 && term.size > math.MaxUint64/term.count {
			return 0, ErrBudget
		}
		value := term.count * term.size
		if value > math.MaxUint64-total {
			return 0, ErrBudget
		}
		total += value
	}
	return total, nil
}
