package graph

import (
	"context"
	"fmt"
)

// Graph is a graph data structure.
type Graph struct {
	// nodes is a map of nodes in the graph.
	nodes map[string]*node

	// starters is a map of nodes that have no parents.
	starters map[string]bool

	// finishers is a map of nodes that have no children.
	finishers map[string]bool
}

// NewGraph creates a new graph.
func NewGraph() Graph {
	return Graph{
		nodes:     make(map[string]*node),
		starters:  make(map[string]bool),
		finishers: make(map[string]bool),
	}
}

// AddNode adds a node to the graph.
func (g Graph) AddNode(key string, impl interface{}) {
	if _, ok := impl.(ExecutableNode); ok {
		g.nodes[key] = &node{
			key:  key,
			impl: impl,
		}
		g.starters[key] = true
		g.finishers[key] = true
		return
	}

	if _, ok := impl.(ExpandableNode); ok {
		g.nodes[key] = &node{
			key:  key,
			impl: impl,
		}
		g.starters[key] = true
		g.finishers[key] = true
		return
	}

	panic(fmt.Errorf("node %q does not implement ExecutableNode or ExpandableNode", key))
}

// Connect connects two nodes in the graph.
func (g Graph) Connect(from string, to string) {
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

	delete(g.starters, to)
	delete(g.finishers, from)
}

// Starters returns the keys of the nodes that have no parents.
func (g Graph) Starters() []string {
	starters := make([]string, 0, len(g.starters))
	for key := range g.starters {
		starters = append(starters, key)
	}
	return starters
}

// Finishers returns the keys of the nodes that have no children.
func (g Graph) Finishers() []string {
	finishers := make([]string, 0, len(g.finishers))
	for key := range g.finishers {
		finishers = append(finishers, key)
	}
	return finishers
}

func (g Graph) Walk(ctx context.Context, parallelism int) error {
	var walker walker
	return walker.Walk(ctx, g, parallelism)
}
