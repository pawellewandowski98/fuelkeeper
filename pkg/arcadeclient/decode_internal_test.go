package arcadeclient

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jsonPadded returns a syntactically valid JSON object of exactly totalLen
// bytes: {"pad":"xxxx...x"}.
func jsonPadded(totalLen int) []byte {
	const prefix = `{"pad":"`
	const suffix = `"}`
	pad := totalLen - len(prefix) - len(suffix)
	if pad < 0 {
		panic("jsonPadded: totalLen too small")
	}
	return []byte(prefix + strings.Repeat("x", pad) + suffix)
}

// TestDecodeJSON_TruncationBoundary is a white-box test (package arcadeclient,
// not arcadeclient_test) exercising decodeJSON's "lr.N <= 0" branch directly,
// for both the 1 MiB success-body limit and the 64 KiB error-body limit. A
// black-box test can observe this for the success path (a real Policy/GetTx
// call surfaces the decode error), but parseRejectError deliberately treats
// a 4xx body's decode error as best-effort and swallows it, so the 64 KiB
// limit's truncation branch is only directly observable here.
func TestDecodeJSON_TruncationBoundary(t *testing.T) {
	tests := []struct {
		name  string
		limit int64
	}{
		{"1 MiB success-body limit", maxSuccessBody},
		{"64 KiB error-body limit", maxErrorBody},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var atLimitDst struct {
				Pad string `json:"pad"`
			}
			atLimit := jsonPadded(int(tt.limit))
			require.Len(t, atLimit, int(tt.limit))
			require.NoError(t, decodeJSON(strings.NewReader(string(atLimit)), tt.limit, &atLimitDst),
				"a body exactly at the limit must still decode")

			var overLimitDst struct {
				Pad string `json:"pad"`
			}
			overLimit := jsonPadded(int(tt.limit) + 1)
			require.Len(t, overLimit, int(tt.limit)+1)
			err := decodeJSON(strings.NewReader(string(overLimit)), tt.limit, &overLimitDst)
			require.Error(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("exceeds %d byte limit", tt.limit))
		})
	}
}
