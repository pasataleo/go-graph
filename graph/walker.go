package graph

import (
	"context"
	"fmt"
	"sync"

	"github.com/pasataleo/go-errors/errors"
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

func (walker *walker) Process(count int) []string {
	var ready []string
	for key := range walker.pending {
		if len(ready) == count {
			break
		}

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

func (walker *walker) Walk(ctx context.Context, graph Graph, parallelism int) error {
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

	var wg sync.WaitGroup
	wg.Add(parallelism)

	// errored, expanded, and completed are channels that the worker will send messages back to indicating the status of a
	// node.
	errored := make(chan map[string]error, 1)
	expanded := make(chan map[string]Graph, 1)
	completed := make(chan string, 1)

	// ready is a channel that the main thread will use to send messages to the workers indicating that a node is ready to
	// be processed.
	ready := make(chan string, parallelism)

	worker := &worker{
		walker:    walker,
		errored:   errored,
		expanded:  expanded,
		completed: completed,
	}

	for _, key := range walker.Process(parallelism) {
		ready <- key
	}
	for i := 0; i < parallelism; i++ {
		go worker.work(ctx, ready, &wg)
	}

	for !walker.Empty() {
		select {
		case errored := <-errored:
			for key, err := range errored {
				logf(ctx, "node %q errored: %v", key, err)
				walker.Errored(key, err)
			}

			for _, key := range walker.Process(parallelism - len(ready)) {
				ready <- key
			}
		case expanded := <-expanded:
			for key, subgraph := range expanded {
				logf(ctx, "node %q expanded", key)

				pending := walker.Expand(key, subgraph)
				if len(pending) == 0 {
					pending = walker.Completed(key)
				}
				for _, starter := range pending {
					walker.pending[starter] = true
				}
			}

			for _, key := range walker.Process(parallelism - len(ready)) {
				ready <- key
			}
		case completed := <-completed:
			logf(ctx, "node %q completed", completed)

			pending := walker.Completed(completed)
			for _, key := range pending {
				walker.pending[key] = true
			}

			for _, key := range walker.Process(parallelism - len(ready)) {
				ready <- key
			}
		}
	}

	close(ready)
	close(errored)
	close(expanded)
	close(completed)

	// Now, wait until all the workers are done.
	wg.Wait()

	// If there are any errors, return them.
	var multi error
	for _, err := range walker.errored {
		multi = errors.Append(err)
	}

	if len(walker.nodes) != (len(walker.completed) + len(walker.errored)) {
		multi = errors.Append(fmt.Errorf("not all nodes completed"))
	}

	return multi
}
