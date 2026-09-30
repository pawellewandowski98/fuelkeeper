// Package openapi embeds the FuelKeeper OpenAPI document.
package openapi

import _ "embed"

// Spec is the OpenAPI 3.0.3 document, served at GET /openapi.yaml.
//
//go:embed fuelkeeper.yaml
var Spec []byte
