package jobs_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/jobs"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

type countingJob struct {
	name string
	runs int
	err  error
}

func (j *countingJob) Name() string            { return j.name }
func (j *countingJob) Interval() time.Duration { return time.Second }
func (j *countingJob) RunOnce(context.Context) error {
	j.runs++
	return j.err
}

func TestRunnerSingleHolder(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	a := jobs.NewRunner(st, "inst-a", 10*time.Second, slog.Default())
	b := jobs.NewRunner(st, "inst-b", 10*time.Second, slog.Default())
	ja, jb := &countingJob{name: "reaper"}, &countingJob{name: "reaper"}

	ran, err := a.Tick(ctx, ja)
	require.NoError(t, err)
	assert.True(t, ran)
	ran, err = b.Tick(ctx, jb)
	require.NoError(t, err)
	assert.False(t, ran, "lease held by a")
	assert.Equal(t, 1, ja.runs)
	assert.Equal(t, 0, jb.runs)

	clk.Advance(5 * time.Second)
	ran, err = a.Tick(ctx, ja)
	require.NoError(t, err)
	assert.True(t, ran, "holder renews")

	clk.Advance(11 * time.Second) // a's lease expired
	ran, err = b.Tick(ctx, jb)
	require.NoError(t, err)
	assert.True(t, ran, "expired lease taken over")
	ran, err = a.Tick(ctx, ja)
	require.NoError(t, err)
	assert.False(t, ran)

	jb.err = errors.New("boom")
	ran, err = b.Tick(ctx, jb)
	assert.True(t, ran)
	require.Error(t, err, "job errors surface from Tick")

	other := &countingJob{name: "watcher"}
	ran, err = a.Tick(ctx, other)
	require.NoError(t, err)
	assert.True(t, ran, "different job name, different lease")

	require.NoError(t, b.ReleaseAll(ctx))
	ran, err = a.Tick(ctx, ja)
	require.NoError(t, err)
	assert.True(t, ran, "released lease is free")
}

func TestRunnerRunTicksAndReleases(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	r := jobs.NewRunner(st, "inst-a", 10*time.Second, slog.Default())
	j := &fastJob{name: "reaper", ran: make(chan struct{}, 8)}
	r.Add(j)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	<-j.ran // first tick runs immediately
	<-j.ran // and again on the interval
	cancel()
	require.NoError(t, <-done)
	// The lease was released on shutdown: another holder can take it now.
	until, err := st.TryAcquireOrRenew(context.Background(), "jobs/reaper", "inst-b", time.Minute)
	require.NoError(t, err)
	assert.False(t, until.IsZero(), "lease released on shutdown")
}

type fastJob struct {
	name string
	ran  chan struct{}
}

func (j *fastJob) Name() string            { return j.name }
func (j *fastJob) Interval() time.Duration { return 5 * time.Millisecond }
func (j *fastJob) RunOnce(context.Context) error {
	select {
	case j.ran <- struct{}{}:
	default:
	}
	return nil
}
