package client

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewDefaultTransportTuning is a white-box test (Client.hc is
// unexported) covering New's default transport: it must be a genuine clone
// of http.DefaultTransport -- keeping its dial and TLS handshake timeouts
// rather than a fresh, zero-valued *http.Transport with neither -- with only
// MaxIdleConnsPerHost and MaxIdleConns raised.
func TestNewDefaultTransportTuning(t *testing.T) {
	c, err := New("http://example.invalid")
	require.NoError(t, err)
	tr, ok := c.hc.Transport.(*http.Transport)
	require.True(t, ok)

	assert.Equal(t, 256, tr.MaxIdleConnsPerHost)
	assert.Equal(t, 512, tr.MaxIdleConns)

	def, ok := http.DefaultTransport.(*http.Transport)
	require.True(t, ok)
	assert.Equal(t, def.TLSHandshakeTimeout, tr.TLSHandshakeTimeout, "TLS handshake timeout should come from http.DefaultTransport")
	assert.Equal(t, def.ExpectContinueTimeout, tr.ExpectContinueTimeout, "should come from http.DefaultTransport")
	assert.Equal(t, def.IdleConnTimeout, tr.IdleConnTimeout, "should come from http.DefaultTransport")
	assert.NotNil(t, tr.DialContext, "dial timeout/keep-alive should come from http.DefaultTransport, not a zero-valued Transport")
	assert.NotNil(t, tr.Proxy, "proxy support should come from http.DefaultTransport")
}
