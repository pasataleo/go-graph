package graph

import (
	"context"

	"github.com/pasataleo/go-errors/errors"
	"github.com/pasataleo/go-threading/threading"
)

type walker struct {
	//sync.Mutex

	// nodes is used to look up nodes by key.
	nodes map[string]*node

	// pending is a map of nodes that are pending execution.
	pending map[string]bool

	// processing is a map of nodes that are currently being processed.
	processing map[string]bool

	// completed is a map of nodes that have finished.
	completed map[string]bool

	// errored is a map of nodes that have errored.
	errored map[string]error

	// subgraphStarters keeps track of all the nodes that started a subgraph, mapped to the nodes that finish it.
	subgraphStarters map[string][]string

	// subgraphFinishers keeps track of all the nodes that finish a subgraph, mapped to the node that started it.
	subgraphFinishers map[string]string
}

func (walker *walker) Process() []string {
	var ready []string
	for key := range walker.pending {
		ready = append(ready, key)
		delete(walker.pending, key)
		walker.processing[key] = true
	}
	return ready
}

func (walker *walker) Empty() bool {
	return len(walker.pending) == 0 && len(walker.processing) == 0
}

func (walker *walker) Errored(key string, err error) {
	walker.errored[key] = err
	delete(walker.processing, key)
}

func (walker *walker) Expand(key string, subgraph Graph) []string {
	delete(walker.processing, key)
	for key, node := range subgraph.nodes {
		walker.nodes[key] = node
	}

	walker.subgraphStarters[key] = subgraph.Finishers()
	for _, finisher := range subgraph.Finishers() {
		walker.subgraphFinishers[finisher] = key
	}

	starters := subgraph.Starters()
	return starters
}

func (walker *walker) Completed(key string) []string {
	walker.completed[key] = true   // First, mark the node as completed.
	delete(walker.processing, key) // Then, remove it from the pending list.

	// Second, we're going to check if this is a finisher for any subgraphs.
	if starter, ok := walker.subgraphFinishers[key]; ok {
		// It is! That means we need to check if all the finishers have been completed.
		starterComplete := true
		for _, finisher := range walker.subgraphStarters[starter] {
			if !walker.completed[finisher] {
				starterComplete = false
				break
			}
		}

		if starterComplete {
			// If all the finishers for the starter have been completed, then we can finally mark the starter as complete.
			return walker.Completed(starter)
		}
	}

	// If we're a "real" node, then we can check if all the children are ready to be executed.
	var ready []string
	for _, child := range walker.nodes[key].children {
		// If all the parents of the child have been completed, then we can add it to the ready list.
		allParentsComplete := true
		for _, parent := range walker.nodes[child].parents {
			if !walker.completed[parent] {
				allParentsComplete = false
				break
			}
		}

		if allParentsComplete {
			ready = append(ready, child)
		}
	}
	return ready
}

func (walker *walker) Walk(ctx context.Context, graph Graph, opts *Opts) error {
	if len(graph.nodes) == 0 {
		return nil
	}

	walker.nodes = make(map[string]*node, len(graph.nodes))
	for key, node := range graph.nodes {
		walker.nodes[key] = node
	}

	walker.pending = make(map[string]bool)
	for _, key := range graph.Starters() {
		walker.pending[key] = true
	}

	walker.processing = make(map[string]bool)
	walker.completed = make(map[string]bool)
	walker.errored = make(map[string]error)
	walker.subgraphStarters = make(map[string][]string)
	walker.subgraphFinishers = make(map[string]string)

	// errored, expanded, and completed are channels that the worker will send messages back to indicating the status of a
	// node.
	errored := make(chan map[string]error, 1)
	expanded := make(chan map[string]Graph, 1)
	completed := make(chan string, 1)

	worker := &worker{
		walker:    walker,
		errored:   errored,
		expanded:  expanded,
		completed: completed,
	}

	pool := threading.NewThreadPool(opts.Parallelism)
	for _, key := range walker.Process() {
		threading.Run(context.WithValue(ctx, "key", key), pool, worker.work)
	}

	for !walker.Empty() {
		select {
		case errored := <-errored:
			for key, err := range errored {
				opts.Callbacks.OnError(key, err)
				walker.Errored(key, err)
			}

			for _, key := range walker.Process() {
				threading.Run(context.WithValue(ctx, "key", key), pool, worker.work)
			}
		case expanded := <-expanded:
			for key, subgraph := range expanded {
				opts.Callbacks.OnExpand(key)

				pending := walker.Expand(key, subgraph)
				if len(pending) == 0 {
					pending = walker.Completed(key)
				}
				for _, starter := range pending {
					walker.pending[starter] = true
				}
			}

			for _, key := range walker.Process() {
				threading.Run(context.WithValue(ctx, "key", key), pool, worker.work)
			}
		case completed := <-completed:
			opts.Callbacks.OnComplete(completed)

			pending := walker.Completed(completed)
			for _, key := range pending {
				walker.pending[key] = true
			}

			for _, key := range walker.Process() {
				threading.Run(context.WithValue(ctx, "key", key), pool, worker.work)
			}
		}
	}

	// Close the channels.
	close(errored)
	close(expanded)
	close(completed)

	// Close the thread pool.
	pool.Close()

	// If there are any errors, return them.
	var multi error
	for _, err := range walker.errored {
		multi = errors.Append(err)
	}

	if len(walker.nodes) != (len(walker.completed) + len(walker.errored)) {
		err := errors.New(nil, IncompleteGraph, "graph is incomplete")
		err = errors.Embed(err, NodeCount, len(walker.nodes))
		err = errors.Embed(err, CompletedCount, len(walker.completed))
		err = errors.Embed(err, ErroredCount, len(walker.errored))
		multi = errors.Append(errors.Append(multi, err))
	}

	return multi
}
