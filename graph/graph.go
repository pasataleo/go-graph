package graph

import (
	"context"
	"fmt"

	"github.com/pasataleo/go-errors/errors"
)

// Key is a key in the graph.
type Key string

// Graph is a graph data structure.
type Graph struct {
	// nodes is a map of nodes in the graph.
	nodes map[Key]*node
}

// NewGraph creates a new graph.
func NewGraph() Graph {
	return Graph{
		nodes: make(map[Key]*node),
	}
}

// AddNode adds a node to the graph.
func (g Graph) AddNode(key Key, impl interface{}) {
	if _, ok := impl.(ExecutableNode); ok {
		g.nodes[key] = &node{
			key:  key,
			impl: impl,
		}
		return
	}

	if _, ok := impl.(ExpandableNode); ok {
		g.nodes[key] = &node{
			key:  key,
			impl: impl,
		}
		return
	}

	panic(fmt.Errorf("node %q does not implement ExecutableNode or ExpandableNode", key))
}

// Connect connects two nodes in the graph.
func (g Graph) Connect(from Key, to Key) {
	if from == to {
		panic(fmt.Errorf("cannot connect node %q to itself", from))
	}

	if _, ok := g.nodes[from]; !ok {
		panic(fmt.Errorf("node %q does not exist", from))
	}

	if _, ok := g.nodes[to]; !ok {
		panic(fmt.Errorf("node %q does not exist", to))
	}

	g.nodes[from].children = append(g.nodes[from].children, to)
	g.nodes[to].parents = append(g.nodes[to].parents, from)
}

// Walk walks the graph.
func (g Graph) Walk(ctx context.Context) error {
	if len(g.nodes) == 0 {
		return nil
	}

	// validate the graph to make sure there are no cycles.
	// we also get a duplicate set of nodes that we can modify.
	fringe, _, nodes, err := g.validate()
	if err != nil {
		return fmt.Errorf("failed to validate graph: %w", err)
	}

	// complete keeps track of the nodes that have been completed. Once a node has been completed, we'll check all
	// the children of the node to see if they can be added to the fringe. They can only be added if all their parents
	// have been completed.
	complete := make(map[Key]bool)
	errored := make(map[Key]error)

	// subgraphStarters keeps track of all the nodes that started a subgraph, mapped to the nodes that finish it.
	subgraphStarters := make(map[Key][]Key)

	// subgraphFinishers keeps track of all the nodes that finish a subgraph, mapped to the node that started it.
	subgraphFinishers := make(map[Key]Key)

	for len(fringe) > 0 {

		// Use the fringe as a queue, to perform a breadth-first walk.
		current := nodes[fringe[0]]
		fringe = fringe[1:]

		if executor, ok := current.impl.(ExecutableNode); ok {
			if err := executor.Execute(ctx); err != nil {
				errored[current.key] = fmt.Errorf("failed to execute node %q: %w", current.key, err)
				continue
			}
		}

		if expander, ok := current.impl.(ExpandableNode); ok {
			subgraph, err := expander.Expand(ctx)
			if err != nil {
				errored[current.key] = fmt.Errorf("failed to expand node %q: %w", current.key, err)
				continue
			}

			// If the subgraph returned no new nodes, then we'll skip it.
			// Otherwise, we should make not of the new nodes and get the subgraph started.

			if len(subgraph.nodes) > 0 {
				starters, finishers, subgraphNodes, err := subgraph.validate()
				if err != nil {
					errored[current.key] = fmt.Errorf("failed to validate subgraph for node %q: %w", current.key, err)
					continue
				}

				for key, node := range subgraphNodes {
					nodes[key] = node
				}

				subgraphStarters[current.key] = finishers

				for _, node := range starters {
					// this is easy, just add all the starters to the fringe.
					fringe = append(fringe, node)
				}

				for _, key := range finishers {
					// finishers are harder, we'll add the children of the expansion node to the children of each finisher.
					// we'll also keep track of the finishers so that when they are finally completed, we can mark the expansion
					// node as complete and unblock the "real" children of the expansion node.
					subgraphFinishers[key] = current.key
				}

				// don't go on and look at the children for an expander with children, we'll do that when all the finishers
				// are complete.
				continue
			}
		}

		complete[current.key] = true

		// if the node is a finisher, this will be empty. if this is a finisher of a subgraph, then we may need to add
		// the children of the starter node to the fringe.
		children := current.children
		if _, ok := subgraphFinishers[current.key]; ok {
			// if this node is a finisher of a subgraph, and the entire subgraph is finished, then we need to add the
			// children of the starter node to the fringe.
			starter := subgraphFinishers[current.key]
			starterComplete := true
			for _, child := range subgraphStarters[starter] {
				if !complete[child] {
					starterComplete = false
					break
				}
			}
			if starterComplete {
				// Then all the children of the starter node have been completed, so we can add the children of the starter
				// node to the fringe. We do this by swapping the empty children with the children of the starter node.
				complete[starter] = true
				children = nodes[starter].children
			}
		}

	Children:
		for _, next := range children {
			child := nodes[next]

			for _, prev := range child.parents {
				if !complete[prev] {
					// If any of the previous nodes have not been completed, skip this node.
					continue Children
				}
			}

			// If all the previous nodes have been completed, add the node to the fringe.
			fringe = append(fringe, next)
		}
	}

	var multi error
	for _, err := range errored {
		multi = errors.Append(multi, err)
	}
	return multi
}
