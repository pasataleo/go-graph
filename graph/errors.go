package graph

import "github.com/pasataleo/go-errors/errors"

var (
	FailedNode      errors.ErrorCode = "graph.failed_node"
	IncompleteGraph errors.ErrorCode = "graph.incomplete_graph"

	NodeKey        = "graph.key"
	NodeCount      = "graph.nodes"
	CompletedCount = "graph.completed"
	ErroredCount   = "graph.errored"
)
