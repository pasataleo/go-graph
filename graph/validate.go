package graph

import (
	"fmt"
	"sort"
	"strings"
)

// Validate validates the graph and returns an error if it detects any cycles.
func (g Graph) Validate() error {
	visited := make(map[string]bool)
	for key := range g.nodes {
		if err := g.dfs(key, visited, nil); err != nil {
			return err
		}
	}
	return nil
}

// dfs performs a depth-first search on the graph, returning an error if it detects any cycles.
func (g Graph) dfs(current string, visited map[string]bool, path []string) error {
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

	var children []string
	children = append(children, g.nodes[current].children...)

	sort.SliceStable(children, func(i, j int) bool {
		// we sort the children to make sure the error messages are deterministic.
		return children[i] < children[j]
	})

	for _, child := range children {
		// recurse to do a depth-first search, TODO: we could also use a stack if we don't want to recurse.
		if err := g.dfs(child, visited, path); err != nil {
			return err
		}
	}
	return nil
}
