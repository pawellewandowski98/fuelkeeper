package toolbox

import (
	"fmt"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
)

// fuelProtocol is the BRC-42 protocol under which per-pool fuel keys are
// derived. go-sdk requires a protocol name of 5..400 lowercase letters, digits
// and single spaces that does not end in " protocol".
var fuelProtocol = sdk.Protocol{SecurityLevel: sdk.SecurityLevelEveryAppAndCounterparty, Protocol: "fuelkeeper"}

// poolKeyIDPrefix keeps every key id comfortably inside BRC-42's length
// bounds even for one-character pool names.
const poolKeyIDPrefix = "pool "

// poolKey derives the private key that presigns a pool's P2PKH fuel (key id
// "pool <name>", counterparty self). The key is deterministic per root key and
// pool, so a restarted treasury presigns to the same lock.
func poolKey(kd *sdk.KeyDeriver, poolName string) (*ec.PrivateKey, error) {
	if kd == nil {
		return nil, fmt.Errorf("treasury: derive pool key: nil key deriver")
	}
	k, err := kd.DerivePrivateKey(fuelProtocol, poolKeyIDPrefix+poolName, sdk.Counterparty{Type: sdk.CounterpartyTypeSelf})
	if err != nil {
		return nil, fmt.Errorf("treasury: derive pool key: %w", err)
	}
	return k, nil
}
