package graph

import (
	"context"
	"strings"
	"testing"

	"github.com/pasataleo/go-testing/tests"
)

func TestGraph_Walk(t *testing.T) {

	tcs := []struct {
		graph    func(g Graph, builder *strings.Builder) Graph
		expected string
	}{
		{
			graph: func(g Graph, _ *strings.Builder) Graph {
				return g
			},
			expected: "",
		},
		{
			graph: func(g Graph, builder *strings.Builder) Graph {
				g.AddNode("a", Executable(func(ctx context.Context) error {
					builder.WriteString("a")
					return nil
				}))
				return g
			},
			expected: "a",
		},
		{
			graph: func(g Graph, builder *strings.Builder) Graph {
				g.AddNode("a", Executable(func(ctx context.Context) error {
					builder.WriteString("a")
					return nil
				}))
				g.AddNode("b", Executable(func(ctx context.Context) error {
					builder.WriteString("b")
					return nil
				}))
				g.Connect("a", "b")
				return g
			},
			expected: "ab",
		},
		{
			graph: func(g Graph, builder *strings.Builder) Graph {
				g.AddNode("a", Executable(func(ctx context.Context) error {
					builder.WriteString("a")
					return nil
				}))
				g.AddNode("b", Expandable(func(ctx context.Context) (Graph, error) {
					graph := NewGraph()
					graph.AddNode("b1", Executable(func(ctx context.Context) error {
						builder.WriteString("b1")
						return nil
					}))
					graph.AddNode("b2", Executable(func(ctx context.Context) error {
						builder.WriteString("b2")
						return nil
					}))
					graph.Connect("b1", "b2")
					return graph, nil
				}))
				g.AddNode("c", Executable(func(ctx context.Context) error {
					builder.WriteString("c")
					return nil
				}))
				g.Connect("a", "b")
				g.Connect("b", "c")
				return g
			},
			expected: "ab1b2c",
		},
		{
			graph: func(g Graph, builder *strings.Builder) Graph {
				g.AddNode("a", Executable(func(ctx context.Context) error {
					builder.WriteString("a")
					return nil
				}))
				g.AddNode("b", Executable(func(ctx context.Context) error {
					builder.WriteString("b")
					return nil
				}))
				g.AddNode("c", Executable(func(ctx context.Context) error {
					builder.WriteString("c")
					return nil
				}))
				g.Connect("a", "b")
				g.Connect("a", "c")
				g.Connect("b", "c")
				return g
			},
			expected: "abc",
		},
		{
			graph: func(g Graph, builder *strings.Builder) Graph {
				g.AddNode("a", Executable(func(ctx context.Context) error {
					builder.WriteString("a")
					return nil
				}))
				g.AddNode("b", Executable(func(ctx context.Context) error {
					builder.WriteString("b")
					return nil
				}))
				g.AddNode("c", Executable(func(ctx context.Context) error {
					builder.WriteString("c")
					return nil
				}))
				g.AddNode("d", Executable(func(ctx context.Context) error {
					builder.WriteString("d")
					return nil
				}))
				g.Connect("a", "b")
				g.Connect("a", "d")
				g.Connect("b", "c")
				g.Connect("c", "d")
				return g
			},
			expected: "abcd",
		},
		{
			graph: func(g Graph, builder *strings.Builder) Graph {
				g.AddNode("a", Executable(func(ctx context.Context) error {
					builder.WriteString("a")
					return nil
				}))
				g.AddNode("b", Executable(func(ctx context.Context) error {
					builder.WriteString("b")
					return nil
				}))
				g.AddNode("c", Executable(func(ctx context.Context) error {
					builder.WriteString("c")
					return nil
				}))
				g.AddNode("d", Executable(func(ctx context.Context) error {
					builder.WriteString("d")
					return nil
				}))
				g.Connect("a", "b")
				g.Connect("a", "c")
				g.Connect("a", "d")
				return g
			},
			expected: "abcd",
		},
		{
			graph: func(g Graph, builder *strings.Builder) Graph {
				g.AddNode("a", Executable(func(ctx context.Context) error {
					builder.WriteString("a")
					return nil
				}))
				g.AddNode("b", Expandable(func(ctx context.Context) (Graph, error) {
					graph := NewGraph()
					graph.AddNode("b1", Expandable(func(ctx context.Context) (Graph, error) {
						graph := NewGraph()
						graph.AddNode("b11", Executable(func(ctx context.Context) error {
							builder.WriteString("b11")
							return nil
						}))
						graph.AddNode("b12", Executable(func(ctx context.Context) error {
							builder.WriteString("b12")
							return nil
						}))
						graph.Connect("b11", "b12")
						return graph, nil
					}))
					return graph, nil
				}))
				g.AddNode("c", Executable(func(ctx context.Context) error {
					builder.WriteString("c")
					return nil
				}))

				g.Connect("a", "b")
				g.Connect("b", "c")
				return g
			},
			expected: "ab11b12c",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.expected, func(t *testing.T) {
			ctx := AttachLogger(context.Background(), DefaultLogger(t.Logf))

			var builder strings.Builder
			tests.ExecuteE(tc.graph(NewGraph(), &builder).Walk(ctx, 1)).NoError(t)
			tests.Execute(builder.String()).Equal(t, tc.expected)
		})
	}

}

func TestGraph_Validate_Error(t *testing.T) {
	tcs := []struct {
		graph       func(g Graph) Graph
		expectedErr string
	}{
		{
			graph: func(g Graph) Graph {
				g.AddNode("a", Executable(func(ctx context.Context) error {
					return nil
				}))
				g.AddNode("b", Executable(func(ctx context.Context) error {
					return nil
				}))
				g.AddNode("c", Executable(func(ctx context.Context) error {
					return nil
				}))
				g.AddNode("d", Executable(func(ctx context.Context) error {
					return nil
				}))
				g.AddNode("e", Executable(func(ctx context.Context) error {
					return nil
				}))

				g.Connect("a", "b")
				g.Connect("b", "c")
				g.Connect("c", "d")
				g.Connect("d", "e")

				// cycle in the middle of the graph
				g.Connect("d", "b")

				return g
			},
			expectedErr: "found cycle in graph: b -> c -> d -> b",
		},
		{
			graph: func(g Graph) Graph {
				g.AddNode("a", Executable(func(ctx context.Context) error {
					return nil
				}))
				g.AddNode("b", Executable(func(ctx context.Context) error {
					return nil
				}))
				g.AddNode("c", Executable(func(ctx context.Context) error {
					return nil
				}))
				g.Connect("a", "b")
				g.Connect("b", "a")
				g.Connect("b", "c")
				return g
			},
			expectedErr: "found cycle in graph: a -> b -> a",
		},
		{
			graph: func(g Graph) Graph {
				g.AddNode("a", Executable(func(ctx context.Context) error {
					return nil
				}))
				g.AddNode("b", Executable(func(ctx context.Context) error {
					return nil
				}))
				g.AddNode("c", Executable(func(ctx context.Context) error {
					return nil
				}))
				g.Connect("a", "b")
				g.Connect("b", "a")
				g.Connect("c", "a")
				return g
			},
			expectedErr: "found cycle in graph: a -> b -> a",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.expectedErr, func(t *testing.T) {
			tests.ExecuteE(tc.graph(NewGraph()).Validate()).
				MatchesError(t, tc.expectedErr)
		})
	}
}
