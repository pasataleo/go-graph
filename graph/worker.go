package graph

import (
	"context"

	"github.com/pasataleo/go-errors/errors"
)

// worker is a worker that processes nodes in the graph.
type worker struct {
	walker *walker // retain a pointer to the walker.

	// errored notifies the main thread when a node errors.
	errored chan map[string]error

	// expanded notifies the main thread when a node is expanded.
	expanded chan map[string]Graph

	// completed notifies the main thread when a node is complete.
	completed chan string
}

// work processes nodes in the graph. Callers should call this in a goroutine, and can call it multiple times.
func (worker *worker) work(ctx context.Context) {
	key := ctx.Value("key").(string)

	node := worker.walker.nodes[key]

	if executor, ok := node.impl.(ExecutableNode); ok {
		if err := executor.Execute(ctx); err != nil {
			worker.errored <- map[string]error{key: errors.Embed(errors.New(err, FailedNode, "failed to execute node"), NodeKey, key)}
			return
		}
	}

	if expander, ok := node.impl.(ExpandableNode); ok {
		subgraph, err := expander.Expand(ctx)
		if err != nil {
			worker.errored <- map[string]error{key: errors.Embed(errors.New(err, FailedNode, "failed to expand node"), NodeKey, key)}
			return
		}

		worker.expanded <- map[string]Graph{key: subgraph}
		return
	}

	worker.completed <- key
}
