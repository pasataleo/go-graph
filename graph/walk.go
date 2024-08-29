package graph

import (
	"context"
	"fmt"
	"sync"

	"github.com/pasataleo/go-errors/errors"
)

// walker contains the state of the walker.
type walker struct {
	sync.Mutex

	// nodes is used to look up nodes by key.
	nodes map[Key]*node

	// pending is a map of nodes that are pending execution.
	pending map[Key]bool

	// completed is a map of nodes that have finished.
	completed map[Key]bool

	// errored is a map of nodes that have errored.
	errored map[Key]error

	// subgraphStarters keeps track of all the nodes that started a subgraph, mapped to the nodes that finish it.
	subgraphStarters map[Key][]Key

	// subgraphFinishers keeps track of all the nodes that finish a subgraph, mapped to the node that started it.
	subgraphFinishers map[Key]Key
}

// Finished returns true if there are no more pending nodes.
func (w *walker) Finished() bool {
	return len(w.pending) == 0
}

// Pending marks a node as pending.
func (w *walker) Pending(key Key) {
	w.pending[key] = true
}

// Errored marks a node as errored and returns true if there are no more pending nodes.
func (w *walker) Errored(key Key, err error) {
	w.errored[key] = err
	delete(w.pending, key)
}

// Expand marks a node as expanded, and preps the subgraph for execution within this walker.
//
// It returns true if the subgraph was empty.
func (w *walker) Expand(key Key, subgraph Graph) []Key {
	// First, move the current node out of pending, and all the nodes from the subgraph into the main graph.
	delete(w.pending, key)
	for key, node := range subgraph.nodes {
		w.nodes[key] = node
	}

	// We want to keep track of the subgraph finishers, so we can do extra processing when they've finished.
	w.subgraphStarters[key] = subgraph.Finishers()
	for _, finisher := range subgraph.Finishers() {
		w.subgraphFinishers[finisher] = key
	}

	return subgraph.Starters()
}

// Complete marks a node as complete and returns the nodes that are now ready to be executed.
func (w *walker) Complete(key Key) []Key {
	w.completed[key] = true // First, mark the node as completed.
	delete(w.pending, key)  // Then, remove it from the pending list.

	// Second, we're going to check if this is a finisher for any subgraphs.
	if starter, ok := w.subgraphFinishers[key]; ok {
		// It is! That means we need to check if all the finishers have been completed.
		starterComplete := true
		for _, child := range w.subgraphStarters[starter] {
			if !w.completed[child] {
				starterComplete = false
				break
			}
		}

		if starterComplete {
			// If all the finishers for the starter have been completed, then we can finally mark the starter as complete.
			return w.Complete(starter)
		}
	}

	// If we're a "real" node, then we can check if all the children are ready to be executed.
	var ready []Key
	for _, child := range w.nodes[key].children {
		// If all the parents of the child have been completed, then we can add it to the ready list.
		allParentsComplete := true
		for _, parent := range w.nodes[child].parents {
			if !w.completed[parent] {
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

// worker is a worker that processes nodes in the graph.
type worker struct {
	walker *walker // retain a pointer to the walker.

	// errored is a callback that is called when a node errors.
	errored func(Key, error)

	// expanded is a callback that is called when a node is expanded.
	expanded func(Key, Graph)

	// complete is a callback that is called when a node is complete.
	complete func(Key)
}

func (worker *worker) work(ctx context.Context, pending chan Key, wg *sync.WaitGroup) {
	defer wg.Done()

	for key := range pending {
		node := worker.walker.nodes[key]
		if executor, ok := node.impl.(ExecutableNode); ok {
			if err := executor.Execute(ctx); err != nil {
				worker.errored(key, fmt.Errorf("failed to execute node %q: %w", key, err))
				continue
			}
		}

		if expander, ok := node.impl.(ExpandableNode); ok {
			subgraph, err := expander.Expand(ctx)
			if err != nil {
				worker.errored(key, fmt.Errorf("failed to expand node %q: %w", key, err))
				continue
			}

			// Mark this node as expanded instead of complete.
			worker.expanded(key, subgraph)
			continue
		}

		// Mark this node as complete.
		worker.complete(key)
	}
}

// Walk walks the graph in parallel. Any node within a cycle will not be executed. If a node errors, then the walk will
// continue, but will not execute any children of the errored node. Any errors will be returned as a multi-error at
// the end.
//
// Cycles should be evaluated when the graph is created, and not when it is walked. You can use the Validate method to
// check for cycles before calling Walk.
//
// You can call this function with a parallelism of 1 to walk the graph serially.
//
// This function is idempotent, and can be called multiple times.
func (g Graph) Walk(ctx context.Context, parallelism int) error {
	if len(g.nodes) == 0 {
		return nil
	}

	walker := &walker{
		nodes: func() map[Key]*node {
			// We'll copy the nodes into a new map so we can modify it.
			nodes := make(map[Key]*node, len(g.nodes))
			for key, node := range g.nodes {
				nodes[key] = node
			}
			return nodes
		}(),
		pending: func() map[Key]bool {
			// We'll start with all the starters as pending.
			pending := make(map[Key]bool)
			for _, key := range g.Starters() {
				pending[key] = true
			}
			return pending
		}(),
		completed:         make(map[Key]bool),
		errored:           make(map[Key]error),
		subgraphStarters:  make(map[Key][]Key),
		subgraphFinishers: make(map[Key]Key),
	}

	var wg sync.WaitGroup
	wg.Add(parallelism)
	fringe := make(chan Key, parallelism) // buffered channel so it is non-blocking

	worker := &worker{
		walker: walker,
		errored: func(key Key, err error) {
			walker.Lock()
			defer walker.Unlock()

			walker.Errored(key, err)
			if walker.Finished() {
				close(fringe)
			}
		},
		expanded: func(key Key, subgraph Graph) {
			walker.Lock()
			defer walker.Unlock()

			starters := walker.Expand(key, subgraph)
			if len(starters) == 0 {
				// If we didn't actually trigger any new nodes, then we can just mark this node as complete.
				walker.Complete(key)
				return
			}

			// Finally, mark the subgraph starters as ready to be executed.
			for _, starter := range starters {
				walker.Pending(starter)
				fringe <- starter
			}
		},
		complete: func(key Key) {
			walker.Lock()
			defer walker.Unlock()

			ready := walker.Complete(key)
			for _, key := range ready {
				walker.Pending(key)
				fringe <- key
			}

			if walker.Finished() {
				close(fringe)
			}
		},
	}

	for i := 0; i < parallelism; i++ {
		// Start a worker for each count of parallelism.
		go worker.work(ctx, fringe, &wg)
	}

	for key := range walker.pending {
		fringe <- key
	}

	// Now, wait until all nodes have been processed.
	wg.Wait()

	var multi error
	for _, err := range walker.errored {
		multi = errors.Append(multi, err)
	}
	return multi
}
