package api

type RebalanceEvent int

const (
	AssignedPartitions RebalanceEvent = iota
	RevokedPartitions
)

type RebalanceEventHandler func(RebalanceEvent, []Offset) error
