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
	require.NoError(t, r.Add(j))
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

func TestRunnerAddRejectsBadJob(t *testing.T) {
	st := memory.New()
	r := jobs.NewRunner(st, "inst-a", time.Minute, slog.Default())

	err := r.Add(&badJob{name: "reaper", interval: 0})
	require.Error(t, err, "zero interval is rejected")

	err = r.Add(&badJob{name: "reaper", interval: -time.Second})
	require.Error(t, err, "negative interval is rejected")

	err = r.Add(&badJob{name: "", interval: time.Second})
	require.Error(t, err, "empty name is rejected")

	err = r.Add(&badJob{name: "reaper", interval: time.Second})
	require.NoError(t, err, "a well-formed job is accepted")
}

type badJob struct {
	name     string
	interval time.Duration
}

func (j *badJob) Name() string            { return j.name }
func (j *badJob) Interval() time.Duration { return j.interval }
func (j *badJob) RunOnce(context.Context) error {
	return nil
}

// TestRunnerPacesFromCompletion checks that a job whose RunOnce runs longer
// than its interval is never re-run back-to-back: Run waits at least one full
// interval after RunOnce *returns* before starting it again.
func TestRunnerPacesFromCompletion(t *testing.T) {
	const interval = 20 * time.Millisecond
	j := &slowJob{
		name:     "reaper",
		interval: interval,
		sleep:    3 * interval,
		starts:   make(chan time.Time, 4),
		ends:     make(chan time.Time, 4),
	}
	st := memory.New()
	r := jobs.NewRunner(st, "inst-a", time.Minute, slog.Default())
	require.NoError(t, r.Add(j))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	<-j.starts           // first run starts
	end1 := <-j.ends     // first run finishes after ~3*interval
	start2 := <-j.starts // second run starts only after the post-completion wait
	cancel()
	<-j.ends // drain the second run so RunOnce doesn't block shutdown
	require.NoError(t, <-done)

	gap := start2.Sub(end1)
	assert.GreaterOrEqual(t, gap, interval, "next run must not start before the interval elapses after the previous run's completion")
}

type slowJob struct {
	name     string
	interval time.Duration
	sleep    time.Duration
	starts   chan time.Time
	ends     chan time.Time
}

func (j *slowJob) Name() string            { return j.name }
func (j *slowJob) Interval() time.Duration { return j.interval }
func (j *slowJob) RunOnce(context.Context) error {
	j.starts <- time.Now()
	time.Sleep(j.sleep)
	j.ends <- time.Now()
	return nil
}
