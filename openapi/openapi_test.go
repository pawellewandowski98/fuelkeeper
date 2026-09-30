package openapi_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/bsv-blockchain/fuelkeeper/openapi"
	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
)

func TestSpecCoversRoutes(t *testing.T) {
	var doc struct {
		OpenAPI string                    `yaml:"openapi"`
		Paths   map[string]map[string]any `yaml:"paths"`
	}
	require.NoError(t, yaml.Unmarshal(openapi.Spec, &doc))
	require.Equal(t, "3.0.3", doc.OpenAPI)

	// Get routes from the API and compare two-way
	apiRoutes := api.Routes()

	// Convert api.Routes to a map for easier comparison
	routedOps := make(map[string]map[string]bool) // path -> method -> true
	for _, r := range apiRoutes {
		if routedOps[r.Pattern] == nil {
			routedOps[r.Pattern] = make(map[string]bool)
		}
		routedOps[r.Pattern][strings.ToLower(r.Method)] = true
	}

	// Check that every api route is documented (ignoring OPTIONS which is implicit)
	for path, methods := range routedOps {
		ops, ok := doc.Paths[path]
		require.True(t, ok, "route %s from api.Routes() is not documented in paths", path)
		for method := range methods {
			require.Contains(t, ops, method, "method %s %s is routed but not documented", method, path)
		}
	}

	// Check that every documented operation is routed (ignoring OPTIONS)
	for path, ops := range doc.Paths {
		for method := range ops {
			if method == "options" || method == "parameters" {
				continue
			}
			require.True(t, routedOps[path][method], "operation %s %s is documented but not routed", method, path)
		}
	}
}

func TestSpecSchemasExist(t *testing.T) {
	var doc struct {
		Components struct {
			Schemas    map[string]any `yaml:"schemas"`
			Responses  map[string]any `yaml:"responses"`
			Parameters map[string]any `yaml:"parameters"`
		} `yaml:"components"`
	}
	require.NoError(t, yaml.Unmarshal(openapi.Spec, &doc))

	// Collect all $ref values in the document by walking the YAML tree
	refs := make(map[string]bool)
	var walk func(*yaml.Node)
	walk = func(n *yaml.Node) {
		if n == nil {
			return
		}
		if n.Kind == yaml.ScalarNode && n.Tag == "!!str" {
			// Check if it's a component reference (schemas, responses, parameters, headers)
			if strings.HasPrefix(n.Value, "#/components/") {
				refs[n.Value] = true
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

	// Verify all referenced components exist
	for ref := range refs {
		require.True(t, isValidRef(ref, doc), "unresolved reference: %s", ref)
	}
}

// isValidRef checks whether a component reference exists in the document.
// Supports: #/components/schemas/<name>, #/components/responses/<name>, #/components/parameters/<name>
func isValidRef(ref string, doc struct {
	Components struct {
		Schemas    map[string]any `yaml:"schemas"`
		Responses  map[string]any `yaml:"responses"`
		Parameters map[string]any `yaml:"parameters"`
	} `yaml:"components"`
}) bool {
	if strings.HasPrefix(ref, "#/components/schemas/") {
		name := strings.TrimPrefix(ref, "#/components/schemas/")
		_, ok := doc.Components.Schemas[name]
		return ok
	}
	if strings.HasPrefix(ref, "#/components/responses/") {
		name := strings.TrimPrefix(ref, "#/components/responses/")
		_, ok := doc.Components.Responses[name]
		return ok
	}
	if strings.HasPrefix(ref, "#/components/parameters/") {
		name := strings.TrimPrefix(ref, "#/components/parameters/")
		_, ok := doc.Components.Parameters[name]
		return ok
	}
	// For other references (headers, etc), verify via YAML traversal
	return verifyRefInYAML(ref)
}

// verifyRefInYAML checks whether a reference exists anywhere in the full document
func verifyRefInYAML(ref string) bool {
	var doc any
	if err := yaml.Unmarshal(openapi.Spec, &doc); err != nil {
		return false
	}

	// Convert to JSON for easier pointer navigation
	jsonBytes, err := json.Marshal(doc)
	if err != nil {
		return false
	}
	var jsonDoc any
	if err := json.Unmarshal(jsonBytes, &jsonDoc); err != nil {
		return false
	}

	// Navigate JSON pointer: #/components/responses/Error -> ["components"]["responses"]["Error"]
	parts := strings.Split(strings.TrimPrefix(ref, "#/"), "/")
	current := jsonDoc
	for _, part := range parts {
		switch v := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = v[part]
			if !ok {
				return false
			}
		default:
			return false
		}
	}
	return true
}
