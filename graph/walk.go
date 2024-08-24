package graph

import (
	"context"
	"fmt"
	"sync"

	"github.com/pasataleo/go-errors/errors"
)

type walker struct {
	nodes map[Key]*node

	errored  func(Key, error)
	expanded func(Key, []Key, []Key, map[Key]*node) bool
	complete func(Key)
}

func (walker *walker) work(ctx context.Context, pending chan Key, wg *sync.WaitGroup) {
	defer wg.Done()

	for key := range pending {
		node := walker.nodes[key]
		if executor, ok := node.impl.(ExecutableNode); ok {
			if err := executor.Execute(ctx); err != nil {
				walker.errored(key, fmt.Errorf("failed to execute node %q: %w", key, err))
				return
			}
		}

		if expander, ok := node.impl.(ExpandableNode); ok {
			subgraph, err := expander.Expand(ctx)
			if err != nil {
				walker.errored(key, fmt.Errorf("failed to expand node %q: %w", key, err))
				return
			}

			starters, finishers, subgraphNodes, err := subgraph.validate()
			if err != nil {
				walker.errored(key, fmt.Errorf("failed to validate subgraph for node %q: %w", key, err))
				return
			}

			// Mark this node as expanded instead of complete.
			complete := walker.expanded(key, starters, finishers, subgraphNodes)
			if !complete {
				// If the node is not complete, then we can skip the rest of the processing.
				continue
			}
		}

		// Mark this node as complete.
		walker.complete(key)
	}
}

func (g Graph) WalkP(ctx context.Context, parallelism int) error {
	if len(g.nodes) == 0 {
		return nil
	}

	// validate the graph, this just executes in the main thread so no parallelisation.
	starters, _, nodes, err := g.validate()
	if err != nil {
		return fmt.Errorf("failed to validate graph: %w", err)
	}

	// pending is a map of nodes that are pending execution.
	pending := make(map[Key]bool)
	for _, key := range starters {
		pending[key] = true
	}

	// complete and errored are maps of nodes that have completed and errored.
	complete := make(map[Key]bool)
	errored := make(map[Key]error)

	// subgraphStarters keeps track of all the nodes that started a subgraph, mapped to the nodes that finish it.
	subgraphStarters := make(map[Key][]Key)

	// subgraphFinishers keeps track of all the nodes that finish a subgraph, mapped to the node that started it.
	subgraphFinishers := make(map[Key]Key)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	wg.Add(parallelism)
	fringe := make(chan Key, parallelism) // buffered channel so it is non-blocking

	walker := &walker{
		nodes: nodes,
		errored: func(key Key, err error) {
			mutex.Lock()
			defer mutex.Unlock()

			errored[key] = err
			delete(pending, key)

			// If there are no more pending nodes, then we can close the fringe.
			if len(pending) == 0 {
				close(fringe)
			}
		},
		expanded: func(key Key, starters []Key, finishers []Key, subgraphNodes map[Key]*node) bool {
			mutex.Lock()
			defer mutex.Unlock()

			// First, move the current node out of pending, and all the nodes from the subgraph into the main graph.
			delete(pending, key)
			for key, node := range subgraphNodes {
				nodes[key] = node
			}

			// We want to keep track of the subgraph finishers, so we can do extra processing when they've finished.
			subgraphStarters[key] = finishers
			for _, finisher := range finishers {
				subgraphFinishers[finisher] = key
			}

			// we can skip the rest of the processing if there are no starters.
			if len(starters) == 0 {
				return true
			}

			// Finally, mark the subgraph starters as ready to be executed.
			for _, starter := range starters {
				pending[starter] = true
				fringe <- starter
			}

			return false
		},
		complete: func(key Key) {
			mutex.Lock()
			defer mutex.Unlock()

			complete[key] = true
			delete(pending, key)

			current := nodes[key]
			children := current.children

			// Before we process the children, we'll check if this is a finisher for any subgraphs.
			// If it is, then we'll check if all the finishers have been completed, and if they have then we'll
			// swap the children out to be the children of the original expander node.
			if starter, ok := subgraphFinishers[key]; ok {
				starterComplete := true
				for _, child := range subgraphStarters[starter] {
					if !complete[child] {
						starterComplete = false
						break
					}
				}
				if starterComplete {
					complete[starter] = true
					children = nodes[starter].children
				}
			}

		Children:
			for _, child := range children {

				// If all the parents of the child have been completed, then we can add it to the fringe.
				for _, parent := range nodes[child].parents {
					if !complete[parent] {
						// If any parent is not complete, then we can't add the child to the fringe.
						continue Children
					}
				}

				// The child can be added to the fringe.
				pending[child] = true
				fringe <- child
			}

			// If there are no more pending nodes, then we can close the fringe.
			if len(pending) == 0 {
				close(fringe)
			}
		},
	}

	for i := 0; i < parallelism; i++ {
		// Start a worker for each count of parallelism.
		go walker.work(ctx, fringe, &wg)
	}

	for key := range pending {
		fringe <- key
	}

	// Now, wait until all nodes have been processed.
	wg.Wait()

	var multi error
	for _, err := range errored {
		multi = errors.Append(multi, err)
	}
	return multi
}
