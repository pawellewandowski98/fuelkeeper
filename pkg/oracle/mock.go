package oracle

import (
	"context"
	"slices"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// Mock is a controllable oracle for tests.
type Mock struct {
	mu            sync.Mutex
	ops           map[store.Outpoint]Result
	txs           map[chainhash.Hash]TxStatus
	down          bool
	calls         int
	assumeUnspent bool
}

var _ Oracle = (*Mock)(nil)

// NewMock returns an empty mock: everything is Unknown until set.
func NewMock() *Mock {
	return &Mock{ops: map[store.Outpoint]Result{}, txs: map[chainhash.Hash]TxStatus{}}
}

// SetUnspent marks op unspent.
func (m *Mock) SetUnspent(op store.Outpoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops[op] = Result{Outpoint: op, Status: StatusUnspent}
}

// SetSpent marks op spent by txid.
func (m *Mock) SetSpent(op store.Outpoint, txid chainhash.Hash) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := txid
	m.ops[op] = Result{Outpoint: op, Status: StatusSpent, SpendingTxID: &h}
}

// SetConflicting marks op as having a double-spend conflict.
func (m *Mock) SetConflicting(op store.Outpoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops[op] = Result{Outpoint: op, Status: StatusConflicting}
}

// Forget removes any verdict for op (back to Unknown).
func (m *Mock) Forget(op store.Outpoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.ops, op)
}

// SetTx sets the verdict for txid.
func (m *Mock) SetTx(txid chainhash.Hash, st TxStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.txs[txid] = copyTxStatus(st)
}

// SetAssumeUnspent makes Outpoints answer Unspent instead of Unknown for
// outpoints with no verdict. Development/benchmark ("nullnet") mode only: a
// real oracle never guesses.
func (m *Mock) SetAssumeUnspent(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.assumeUnspent = v
}

// SetDown makes every call fail with ErrUnavailable.
func (m *Mock) SetDown(down bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.down = down
}

// Calls returns how many Outpoints/Tx calls were made.
func (m *Mock) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// Outpoints returns the configured verdict for each outpoint, Unknown when unset.
func (m *Mock) Outpoints(_ context.Context, ops []store.Outpoint) ([]Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.down {
		return nil, ErrUnavailable
	}
	out := make([]Result, len(ops))
	for i, op := range ops {
		if r, ok := m.ops[op]; ok {
			if r.SpendingTxID != nil {
				h := *r.SpendingTxID
				r.SpendingTxID = &h
			}
			out[i] = r
		} else {
			status := StatusUnknown
			if m.assumeUnspent {
				status = StatusUnspent
			}
			out[i] = Result{Outpoint: op, Status: status}
		}
	}
	return out, nil
}

// Tx returns the configured status for txid, the zero TxStatus when unset.
func (m *Mock) Tx(_ context.Context, txid chainhash.Hash) (TxStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.down {
		return TxStatus{}, ErrUnavailable
	}
	return copyTxStatus(m.txs[txid]), nil
}

// copyTxStatus returns a copy of st whose slice fields do not alias st's,
// so a caller mutating a returned/stored slice cannot change later answers.
// Inputs is cloned with slices.Clone rather than append-to-nil because an
// empty non-nil Inputs ("verified: spends nothing") must not become nil
// ("unknown, trust the txid"): see TxStatus.Inputs.
func copyTxStatus(st TxStatus) TxStatus {
	if st.MerklePath != nil {
		st.MerklePath = append([]byte(nil), st.MerklePath...)
	}
	if st.CompetingTxs != nil {
		st.CompetingTxs = append([]string(nil), st.CompetingTxs...)
	}
	if st.SpentInputs != nil {
		st.SpentInputs = slices.Clone(st.SpentInputs)
	}
	if st.Inputs != nil {
		st.Inputs = slices.Clone(st.Inputs)
	}
	return st
}
