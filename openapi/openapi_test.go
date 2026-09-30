package openapi_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/bsv-blockchain/fuelkeeper/openapi"
)

func TestSpecCoversRoutes(t *testing.T) {
	var doc struct {
		OpenAPI string                    `yaml:"openapi"`
		Paths   map[string]map[string]any `yaml:"paths"`
	}
	require.NoError(t, yaml.Unmarshal(openapi.Spec, &doc))
	require.Equal(t, "3.0.3", doc.OpenAPI)
	want := map[string][]string{
		"/healthz":                              {"get"},
		"/readyz":                               {"get"},
		"/metrics":                              {"get"},
		"/openapi.yaml":                         {"get"},
		"/v1/pools":                             {"get"},
		"/v1/pools/{pool}":                      {"get"},
		"/v1/pools/{pool}/reserve":              {"post"},
		"/v1/pools/{pool}/release":              {"post"},
		"/v1/pools/{pool}/spend":                {"post"},
		"/v1/pools/{pool}/beef/{txid}":          {"get"},
		"/v1/pools/{pool}/shards":               {"get"},
		"/v1/pools/{pool}/shards/{txid}/retire": {"post"},
		"/v1/pools/{pool}/quarantined":          {"get"},
		"/v1/pools/{pool}/quarantined/requeue":  {"post"},
	}
	for path, methods := range want {
		ops, ok := doc.Paths[path]
		require.True(t, ok, "missing path %s", path)
		for _, m := range methods {
			require.Contains(t, ops, m, "%s %s", m, path)
		}
	}
}

func TestSpecSchemasExist(t *testing.T) {
	var doc struct {
		Components struct {
			Schemas map[string]any `yaml:"schemas"`
		} `yaml:"components"`
	}
	require.NoError(t, yaml.Unmarshal(openapi.Spec, &doc))

	// Collect all $ref values in the document
	refs := make(map[string]bool)
	var walk func(*yaml.Node)
	walk = func(n *yaml.Node) {
		if n == nil {
			return
		}
		if n.Kind == yaml.ScalarNode && n.Tag == "!!str" {
			// Check if it's a schema reference
			if len(n.Value) > len("#/components/schemas/") &&
				n.Value[:len("#/components/schemas/")] == "#/components/schemas/" {
				schemaName := n.Value[len("#/components/schemas/"):]
				refs[schemaName] = true
			}
		}
		if n.Content != nil {
			for _, child := range n.Content {
				walk(child)
			}
		}
	}

	var fullDoc yaml.Node
	require.NoError(t, yaml.Unmarshal(openapi.Spec, &fullDoc))
	walk(&fullDoc)

	// Verify all referenced schemas exist
	for ref := range refs {
		_, ok := doc.Components.Schemas[ref]
		require.True(t, ok, "schema %s is referenced but not defined", ref)
	}
}
