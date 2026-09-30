package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
)

// quarantineFlagNames lists every flag adminCmd defines. Every one of them
// takes a value, which extractSubcommand needs to know so it can tell a
// flag's value apart from the quarantine subcommand regardless of which side
// of the flags the subcommand appears on.
var quarantineFlagNames = map[string]bool{
	"url": true, "key": true, "pool": true, "status": true,
	"limit": true, "token": true, "outpoints": true, "txid": true,
}

// extractSubcommand pulls the first bare (non-flag) argument out of args and
// returns it along with the remaining arguments, in their original relative
// order. This lets "fuelkeeper quarantine" accept its list|requeue|retire
// subcommand either before or after the flag arguments -- flag.FlagSet.Parse
// on its own only tolerates positional arguments after every flag, since it
// stops parsing at the first one it sees.
//
// A flag written as "-name value" or "--name value" (as opposed to
// "--name=value") consumes the following token as its value even though that
// token does not itself start with "-", so such a pair is never mistaken for
// the subcommand.
func extractSubcommand(args []string) (sub string, rest []string) {
	rest = make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case !strings.HasPrefix(a, "-"):
			if sub == "" {
				sub = a
				continue
			}
			rest = append(rest, a)
		case strings.Contains(a, "="):
			rest = append(rest, a)
		default:
			rest = append(rest, a)
			name := strings.TrimLeft(a, "-")
			if quarantineFlagNames[name] && i+1 < len(args) {
				i++
				rest = append(rest, args[i])
			}
		}
	}
	return sub, rest
}

// adminCmd runs one admin subcommand (pools|shards|quarantine|release)
// against the FuelKeeper HTTP API and prints its result as indented JSON on
// stdout.
func adminCmd(ctx context.Context, name string, args []string) error {
	var sub string
	if name == "quarantine" {
		sub, args = extractSubcommand(args)
		if sub == "" {
			sub = "list"
		}
	}

	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	url := fs.String("url", "http://127.0.0.1:8080", "FuelKeeper base URL")
	key := fs.String("key", os.Getenv("FUELKEEPER_API_KEY"), "API key (or $FUELKEEPER_API_KEY)")
	poolName := fs.String("pool", "main", "pool name")
	status := fs.String("status", "", "shard status filter (unmined|mined|retired|invalid)")
	limit := fs.Int("limit", 100, "page size")
	token := fs.String("token", "", "reservation token")
	outpoints := fs.String("outpoints", "", "comma-separated outpoints txid:vout")
	txid := fs.String("txid", "", "shard txid")
	if err := fs.Parse(args); err != nil {
		return err
	}

	c, err := client.New(*url, client.WithAPIKey(*key))
	if err != nil {
		return err
	}
	out := json.NewEncoder(os.Stdout)
	out.SetIndent("", "  ")
	splitOutpoints := func() []string {
		if *outpoints == "" {
			return nil
		}
		return strings.Split(*outpoints, ",")
	}

	switch name {
	case "pools":
		v, err := c.Pools(ctx)
		if err != nil {
			return err
		}
		return out.Encode(v)
	case "shards":
		v, err := c.Shards(ctx, *poolName, *status, *limit)
		if err != nil {
			return err
		}
		return out.Encode(v)
	case "release":
		n, err := c.Release(ctx, *poolName, *token, splitOutpoints())
		if err != nil {
			return err
		}
		return out.Encode(map[string]int{"released": n})
	case "quarantine":
		return quarantineCmd(ctx, c, out, sub, *poolName, *limit, splitOutpoints(), *txid)
	default:
		return fmt.Errorf("unknown command %q", name)
	}
}

// quarantineCmd dispatches the quarantine subcommand (list|requeue|retire).
func quarantineCmd(ctx context.Context, c *client.Client, out *json.Encoder, sub, poolName string, limit int, outpoints []string, txid string) error {
	switch sub {
	case "list":
		v, err := c.Quarantined(ctx, poolName, limit)
		if err != nil {
			return err
		}
		return out.Encode(v)
	case "requeue":
		n, err := c.Requeue(ctx, poolName, outpoints)
		if err != nil {
			return err
		}
		return out.Encode(map[string]int{"requeued": n})
	case "retire":
		n, err := c.RetireShard(ctx, poolName, txid)
		if err != nil {
			return err
		}
		return out.Encode(map[string]int{"retired": n})
	default:
		return fmt.Errorf("unknown quarantine subcommand %q (want list|requeue|retire)", sub)
	}
}
