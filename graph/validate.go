package graph

import (
	"fmt"
	"sort"
	"strings"
)

// validate validates the graph, and returns a duplicate set of nodes that can be modified by whatever
// operation is about to be started.
//
// This function makes sure there are no cycles in the graph.
func (g Graph) validate() ([]Key, []Key, map[Key]*node, error) {
	nodes := make(map[Key]*node, len(g.nodes))
	var starters []Key
	var finishers []Key

	visited := make(map[Key]bool)
	for key, node := range g.nodes {
		if len(node.parents) == 0 {
			starters = append(starters, key)
		}
		if len(node.children) == 0 {
			finishers = append(finishers, key)
		}

		nodes[key] = node
		if err := g.dfs(key, visited, nil); err != nil {
			return nil, nil, nil, err
		}
	}

	// Make our future iterations stable.

	sort.SliceStable(starters, func(i, j int) bool {
		return starters[i] < starters[j]
	})
	sort.SliceStable(finishers, func(i, j int) bool {
		return finishers[i] < finishers[j]
	})

	return starters, finishers, nodes, nil
}

// dfs performs a depth-first search on the graph, returning an error if it detects any cycles.
func (g Graph) dfs(current Key, visited map[Key]bool, path []string) error {
	for ix, ancestor := range path {
		if ancestor == string(current) {
			// Then we have a cycle.
			return fmt.Errorf("found cycle in graph: %s", strings.Join(append(path[ix:], string(current)), " -> "))
		}
	}

	if visited[current] {
		// If we've visited this node before, then we're done. We'd have detected a cycle already.
		return nil
	}

	visited[current] = true
	path = append(path, string(current))
	for _, child := range g.nodes[current].children {
		// recurse to do a depth-first search, TODO: we could also use a stack if we don't want to recurse.
		if err := g.dfs(child, visited, path); err != nil {
			return err
		}
	}
	return nil
}
