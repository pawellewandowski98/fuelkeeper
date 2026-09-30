package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseOutpointsTrimsWhitespace checks that stray whitespace around
// comma-separated --outpoints items (and a stray leading/trailing/doubled
// comma) never reaches the server as part of an outpoint string.
func TestParseOutpointsTrimsWhitespace(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{name: "no whitespace", raw: "a:0,b:1", want: []string{"a:0", "b:1"}},
		{name: "spaces around items", raw: "a:0, b:1 , c:2", want: []string{"a:0", "b:1", "c:2"}},
		{name: "tabs and stray commas collapse to nothing", raw: " a:0 ,\t, b:1,", want: []string{"a:0", "b:1"}},
		{name: "empty string", raw: "", want: nil},
		{name: "all whitespace", raw: "   ", want: nil},
		{name: "single item with padding", raw: "  a:0  ", want: []string{"a:0"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseOutpoints(tt.raw))
		})
	}
}

// TestValidateAdminArgs checks each subcommand's required-flag rules and that
// every failure is a usageError (so main exits 2, not 1).
func TestValidateAdminArgs(t *testing.T) {
	tests := []struct {
		name          string
		cmd           string
		quarantineSub string
		token         string
		txid          string
		outpoints     []string
		wantErr       bool
	}{
		{name: "release ok", cmd: "release", token: "tok", outpoints: []string{"a:0"}, wantErr: false},
		{name: "release missing token", cmd: "release", outpoints: []string{"a:0"}, wantErr: true},
		{name: "release missing outpoints", cmd: "release", token: "tok", wantErr: true},
		{name: "quarantine list needs nothing", cmd: "quarantine", quarantineSub: "list", wantErr: false},
		{name: "quarantine requeue ok", cmd: "quarantine", quarantineSub: "requeue", outpoints: []string{"a:0"}, wantErr: false},
		{name: "quarantine requeue missing outpoints", cmd: "quarantine", quarantineSub: "requeue", wantErr: true},
		{name: "quarantine retire ok", cmd: "quarantine", quarantineSub: "retire", txid: "abcd", wantErr: false},
		{name: "quarantine retire missing txid", cmd: "quarantine", quarantineSub: "retire", wantErr: true},
		{name: "quarantine unknown subcommand", cmd: "quarantine", quarantineSub: "wat", wantErr: true},
		{name: "pools needs nothing", cmd: "pools", wantErr: false},
		{name: "shards needs nothing", cmd: "shards", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAdminArgs(tt.cmd, tt.quarantineSub, tt.token, tt.txid, tt.outpoints)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var uerr *usageError
			require.ErrorAs(t, err, &uerr, "expected a usageError, got %T: %v", err, err)
		})
	}
}

// TestAdminCmdMissingRequiredFlagsReturnsUsageError checks the same rules one
// layer up, through adminCmd itself: a missing required flag must fail
// before any network call is attempted (no server is running in this test),
// with an error that unwraps to usageError so main would exit 2.
func TestAdminCmdMissingRequiredFlagsReturnsUsageError(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		cmd  string
		args []string
	}{
		{name: "release without token", cmd: "release", args: []string{"--pool", "main", "--outpoints", "a:0"}},
		{name: "release without outpoints", cmd: "release", args: []string{"--pool", "main", "--token", "tok"}},
		{name: "quarantine requeue without outpoints", cmd: "quarantine", args: []string{"requeue", "--pool", "main"}},
		{name: "quarantine retire without txid", cmd: "quarantine", args: []string{"retire", "--pool", "main"}},
		{name: "quarantine unknown subcommand", cmd: "quarantine", args: []string{"bogus", "--pool", "main"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := adminCmd(ctx, tt.cmd, tt.args)
			require.Error(t, err)
			var uerr *usageError
			require.ErrorAs(t, err, &uerr, "expected a usageError, got %T: %v", err, err)
		})
	}
}

// TestAdminCmdBadFlagIsUsageError checks that a flag.FlagSet.Parse failure
// (an unknown flag, a malformed value) is also a usageError, not a bare
// parse error.
func TestAdminCmdBadFlagIsUsageError(t *testing.T) {
	err := adminCmd(context.Background(), "pools", []string{"--not-a-real-flag"})
	require.Error(t, err)
	var uerr *usageError
	require.ErrorAs(t, err, &uerr, "expected a usageError, got %T: %v", err, err)
}

// TestServeCmdBadFlagIsUsageError mirrors the above for "serve".
func TestServeCmdBadFlagIsUsageError(t *testing.T) {
	err := serveCmd(context.Background(), []string{"--not-a-real-flag"})
	require.Error(t, err)
	var uerr *usageError
	require.ErrorAs(t, err, &uerr, "expected a usageError, got %T: %v", err, err)
}
