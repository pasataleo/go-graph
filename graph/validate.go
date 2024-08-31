package graph

import (
	"fmt"
	"sort"
	"strings"
)

// Validate validates the graph and returns an error if it detects any cycles.
func (g Graph) Validate() error {
	var keys []string
	for key := range g.nodes {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	visited := make(map[string]bool)
	for _, key := range keys {
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

	sort.Strings(children)
	for _, child := range children {
		if err := g.dfs(child, visited, path); err != nil {
			return err
		}
	}
	return nil
}
