# Funding the Treasury

FuelKeeper mints fuel from its own wallet balance. This guide covers how an
operator tops that balance up: a [BRC-29](https://github.com/bitcoin-sv/BRCs/blob/master/wallet/0029.md)
wallet-to-wallet payment into the embedded go-wallet-toolbox treasury, driven
from the operator's own local BRC-100 wallet.

## What Funding Is

FuelKeeper's treasury (`pkg/treasury/toolbox`) embeds a go-wallet-toolbox
wallet. It does not mine or receive coinbase; it only spends what it already
holds. An operator funds it the same way any BRC-100 wallet receives a
payment: derive a one-time BRC-29 key from the treasury's identity key,
pay a P2PKH output to it, then hand the payment to the treasury so it can
`internalizeAction` it into its own balance.

Once internalized, the satoshis are just wallet balance. They do not become
fuel outputs directly:

1. The chunk keeper (go-wallet-toolbox's `pkg/wallet/fuelkeeper.Keeper`, run
   from `Core.EnsureChunks`) fans the wallet's spendable balance out into a
   reserve basket, then further into a basket of exact-denomination
   **leaf-sized chunks** sized to cover one leaf split's outputs plus its fee
   (`Config.ChunkValue`, i.e. `LeafCost` plus headroom).
2. The minter consumes one leaf-sized chunk per **leaf split**: a transaction
   whose outputs are the pool's fuel denomination, repeated `split_size`
   times (`Core.Build`), broadcast once recorded (`Core.Send`).
3. That split becomes a shard, `unmined` until the oracle proves it mined,
   at which point its rows become available for reservation.

**One treasury funds one leaf-cost class.** The toolbox keeps a single chunk
denomination per wallet storage, so every pool a given FuelKeeper instance
serves must resolve to the same leaf cost (`split_size × denomination + leaf
fee`); the server refuses to start otherwise. Run one FuelKeeper instance per
leaf-cost class. See `docs/TREASURY.md` for the toolbox wiring, storage and
chunk-keeper configuration in full.

## The One-Liner

```
fuelkeeper fund --sats 100000 [--url http://127.0.0.1:8080] [--key K | FUELKEEPER_API_KEY] [--wallet http://localhost:3321] [--output-index 0]
```

This drives your local BRC-100 wallet (e.g. BSV Desktop / Metanet Desktop)
over the go-sdk HTTP JSON substrate, identifying itself to it with originator
`fuelkeeper.cli`. It fetches funding instructions from FuelKeeper, derives a
payment key with the wallet, pays a P2PKH output for `--sats` satoshis, and
posts the completed payment back to FuelKeeper. On success it prints:

```
funded <sats> sats in <txid>
```

### Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--sats` | *(required, must be > 0)* | amount to fund, in satoshis |
| `--url` | `http://127.0.0.1:8080` | FuelKeeper base URL |
| `--key` | `$FUELKEEPER_API_KEY` | API key; **must be an admin key** when the server has `api.admin_keys` configured |
| `--wallet` | `http://localhost:3321` | local BRC-100 wallet base URL |
| `--output-index` | `0` | index of the funding output within the wallet's created transaction; `-1` (auto-detect) is not yet supported and is rejected as `bad_funding` |

Both funding operations are admin-tier: gating them behind an admin key (when
`api.admin_keys` is configured) keeps anonymous callers from spamming
derivation nonces and BEEF at the treasury. With no admin keys configured, a
fuel key (or no key, in development mode) works.

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | funded; the txid and satoshis are printed to stdout |
| `1` | runtime failure: the wallet call failed, or `POST /v1/funding` failed (see Recovery below — the error names the already-broadcast txid, prefix and suffix) |
| `2` | usage error: `--sats` missing or zero, or a malformed `--url` |

## The Flow by Hand

`fuelkeeper fund` is three HTTP/wallet calls. Here they are individually, for
scripting or debugging without the CLI.

### 1. `GET /v1/funding`

```
curl -s http://127.0.0.1:8080/v1/funding \
  -H "Authorization: Bearer $FUELKEEPER_API_KEY"
```

```json
{
  "identity_key": "02ab...",
  "derivation_prefix": "b64nonce==",
  "network": "test",
  "protocol": "3241645161d8",
  "instructions": "Derive a BRC-29 key with GetPublicKey{...}, pay a P2PKH output to it, then POST the AtomicBEEF, prefix, suffix and your identity key here."
}
```

| Field | Meaning |
|-------|---------|
| `identity_key` | the treasury wallet's identity public key, DER hex (66 hex chars) |
| `derivation_prefix` | a fresh base64 nonce; use it as the BRC-29 derivation prefix. It is HMAC-based and not one-shot — see Recovery below |
| `network` | `main` \| `test` \| `ttn` \| `tstn` |
| `protocol` | the fixed BRC-29 protocol ID string, `"3241645161d8"` (security level 2, "every app and counterparty") |
| `instructions` | the same recipe as this section, for tooling that wants it inline |

### 2. Derive a payment key with your own wallet

Call your local BRC-100 wallet's `getPublicKey`:

- `protocolID`: `[2, "3241645161d8"]`
- `keyID`: `"<derivation_prefix> <suffix>"` — `derivation_prefix` from step 1, and `suffix` a fresh base64 value you choose (the CLI uses 8 random bytes)
- `counterparty`: the treasury's `identity_key` from step 1

The wallet returns a public key. Build a P2PKH locking script paying that
key's address for the amount you want to fund.

### 3. Pay with `createAction`

Call your wallet's `createAction` with one output: the locking script from
step 2, your funding amount in satoshis, and `randomizeOutputs: false` (so
the payment output stays at a known, fixed index — `fuelkeeper fund` always
uses index 0). The wallet signs and broadcasts a transaction and returns it
(as AtomicBEEF, together with its txid).

### 4. `POST /v1/funding`

```
curl -s -X POST http://127.0.0.1:8080/v1/funding \
  -H "Authorization: Bearer $FUELKEEPER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "atomic_beef": "<base64 AtomicBEEF of the tx from step 3>",
    "output_index": 0,
    "derivation_prefix": "<derivation_prefix from step 1>",
    "derivation_suffix": "<suffix you chose in step 2>",
    "sender_identity_key": "<your wallet identity key, 66 hex chars>"
  }'
```

```json
{ "txid": "...", "satoshis": 100000 }
```

| Field | Meaning |
|-------|---------|
| `atomic_beef` | base64 AtomicBEEF (the payment transaction plus its ancestry) |
| `output_index` | index of the payment output. Omitting it currently fails (`bad_funding`: auto-detection is not yet supported) — always pass the actual index |
| `derivation_prefix` | base64, must be the same value `GET /v1/funding` returned |
| `derivation_suffix` | base64, the value you chose in step 2 |
| `sender_identity_key` | your wallet's identity public key, 66 hex chars (DER) |

The treasury checks (`pkg/treasury/toolbox/core.go`, `Internalize`): the
AtomicBEEF size (4 MiB cap), that `output_index` is non-negative and in
range, that `derivation_prefix`/`derivation_suffix` decode as non-empty
base64, that the nonce is one this treasury issued, that
`sender_identity_key` parses as a compressed public key, and — before ever
calling the wallet — that the named output's locking script is exactly the
BRC-29 script expected for that prefix, suffix and sender. Only after that
pre-check passes does it call the wallet's `internalizeAction`.

## TypeScript

Illustrative only — field names on the wire must match `pkg/api/wire/wire.go`
exactly (snake_case JSON, as shown above).

```ts
import { WalletClient, P2PKH, PublicKey, Utils } from '@bsv/sdk'

const wallet = new WalletClient()
const info = await (await fetch(`${baseURL}/v1/funding`, { headers })).json()

const { publicKey: pub } = await wallet.getPublicKey({
  protocolID: [2, '3241645161d8'],
  keyID: `${info.derivation_prefix} ${suffix}`,
  counterparty: info.identity_key,
})
const lockingScript = new P2PKH().lock(PublicKey.fromString(pub).toAddress())
const { tx } = await wallet.createAction({
  description: 'Fund FuelKeeper',
  outputs: [{ lockingScript, satoshis, outputDescription: 'FuelKeeper treasury funding' }],
  options: { randomizeOutputs: false },
})

await fetch(`${baseURL}/v1/funding`, {
  method: 'POST',
  headers: { ...headers, 'Content-Type': 'application/json' },
  body: JSON.stringify({
    atomic_beef: Utils.toBase64(tx), output_index: 0,
    derivation_prefix: info.derivation_prefix, derivation_suffix: suffix,
    sender_identity_key: identityKey,
  }),
})
```

## What Happens Next

Once `POST /v1/funding` returns, the payment is wallet balance, not yet fuel.
From there:

1. The chunk keeper fans that balance into reserve chunks, then into
   leaf-sized chunks (defaults below; see `docs/TREASURY.md` for tuning).
2. The minter mints a leaf split from a chunk: `split_size` fuel outputs at
   the pool's denomination, recorded as a shard (`unmined`) before it is
   broadcast.
3. Once the funding transaction — and later, each leaf split — is mined,
   the watcher job marks the corresponding shard `mined` and its rows become
   available to reserve.

Timing depends on the network the treasury is configured against; there is
no fixed wait here beyond "after the funding transaction is mined."

### Chunk Keeper Defaults

| Setting | Default | Meaning |
|---------|---------|---------|
| `fanout_outputs_per_tx` | 100 | chunks created per fan-out transaction |
| `target_chunks` | 50 | leaf-sized chunks kept ready |
| `low_water_percent` | 60 | refill threshold, as a percent of `target_chunks` |
| `headroom_sats` | 20 | extra satoshis per chunk so a leaf split's surplus stays below dust |
| `interval` | 10s | chunk keeper cadence |
| `reserve_basket` | `reserve` | wallet basket holding the fan-out reserve |
| `chunk_basket` | `chunks` | wallet basket holding leaf-sized chunks |

### Watching Progress

```
fuelkeeper pools --url http://127.0.0.1:8080 [--key K]
fuelkeeper shards --url http://127.0.0.1:8080 --pool main --status unmined [--key K]
```

`fuelkeeper pools` reports each pool's `stats.shards` counts
(`unmined`/`mined`/`retired`/`invalid`). `fuelkeeper shards --status unmined`
lists the shards still waiting to be mined; re-run with `--status mined` to
see them move over. Equivalently, `GET /v1/pools` and
`GET /v1/pools/{pool}/shards?status=unmined` give the same data over HTTP.

## Recovery

**`POST /v1/funding` failed after the wallet already broadcast the
payment.** Your funds are not lost and the payment does not need to be made
again. `fuelkeeper fund` prints the already-broadcast txid together with the
`derivation_prefix`/`derivation_suffix` pair in its error message; retry
`POST /v1/funding` with those same two values. The derivation nonce is
HMAC-based, not one-shot, so it stays valid: retrying only re-submits the
same payment for internalization. Never re-run `fuelkeeper fund` from
scratch in this case — that pays a second time. More generally, the nonce
staying valid after use means replaying it (with its matching payment) can
only fund the treasury again; it is not a one-time secret that must be
protected from reuse the way a private key would be.

**Wrong `--output-index` / `output_index`.** The treasury checks that
output's locking script against the expected BRC-29 script for your prefix,
suffix and sender. A mismatch — wrong index, wrong suffix, wrong sender key —
comes back `400 bad_funding`. Fix the value and retry with the same prefix
and suffix; nothing on-chain needs to change.

**`404 funding_disabled`.** This instance has no funding treasury configured
(for example, the nullnet/mock treasury used in development). There is
nothing to fund; see `docs/OPERATIONS.md` for treasury configuration.

**`401 unauthorized`.** Both funding endpoints are admin-tier. Pass an admin
key with `--key` / `FUELKEEPER_API_KEY` when the server has `api.admin_keys`
configured.

**`503 unavailable`.** The treasury is temporarily unable to internalize the
payment (for example, mid-broadcast on another request). Retry; the response
carries `Retry-After`.

## Errors

| Status | Code | Meaning | Retry-After |
|--------|------|---------|-------------|
| 400 | `bad_request` | malformed request (bad base64, or a `sender_identity_key` that is not 66 hex chars) | No |
| 400 | `bad_funding` | the treasury could not verify or accept the payment (wrong derivation, missing or out-of-range output, output does not match the expected BRC-29 script, unknown derivation prefix, invalid sender identity key, oversized AtomicBEEF, ...) | No |
| 401 | `unauthorized` | invalid or missing admin key | No |
| 404 | `funding_disabled` | this instance has no funding treasury configured | No |
| 499 | `client_closed_request` | client disconnected before the response was sent | No |
| 500 | `internal` | server error | No |
| 503 | `unavailable` | the treasury is temporarily unable to internalize the payment | Yes (1s) |

This table applies to both `GET /v1/funding` and `POST /v1/funding`, except
that `GET /v1/funding` never returns `bad_funding` (it carries no payment to
validate). See `openapi/fuelkeeper.yaml` for the full schema.
