package graph

import "context"

// node is a node in the graph.
type node struct {
	// Key is the key of the node.
	key Key

	// impl is the implementation of the node.
	impl interface{}

	// parents and children contain the parents and children of the node.
	parents  []Key
	children []Key
}

// ExecutableNode is a node that can be executed.
type ExecutableNode interface {
	Execute(ctx context.Context) error
}

type executable struct {
	fn func(ctx context.Context) error
}

// Executable creates a new executable node that is just a simple function.
func Executable(fn func(ctx context.Context) error) ExecutableNode {
	return &executable{fn: fn}
}

func (e *executable) Execute(ctx context.Context) error {
	return e.fn(ctx)
}

// ExpandableNode is a node that can be expanded.
type ExpandableNode interface {
	Expand(ctx context.Context) (Graph, error)
}

type expandable struct {
	fn func(ctx context.Context) (Graph, error)
}

// Expandable creates a new expandable node that is just a simple function.
func Expandable(fn func(ctx context.Context) (Graph, error)) ExpandableNode {
	return &expandable{fn: fn}
}

func (e *expandable) Expand(ctx context.Context) (Graph, error) {
	return e.fn(ctx)
}
