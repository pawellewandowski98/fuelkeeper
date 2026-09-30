// Package jobs holds FuelKeeper's background jobs (reaper, watcher, minter) and
// the leased runner that guarantees one active runner per job across instances.
// Every job write is a CAS, so an overlapping run during lease hand-over is harmless.
package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

var jobLeaseHeld = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "fuelkeeper_job_lease_held", Help: "1 when this instance holds the job lease.",
}, []string{"job"})

// Job is a periodic single-runner task. Name must be a short lowercase
// identifier (it is used verbatim in the lease name "jobs/<name>" and as a
// Prometheus label value).
type Job interface {
	// Name identifies the job and its lease; see the Job doc comment.
	Name() string
	// Interval is how often Run ticks the job; it must be > 0 (see Add).
	Interval() time.Duration
	// RunOnce performs one unit of work. Implementations must return promptly
	// when ctx is cancelled: Run waits for every job goroutine to exit before
	// releasing any lease, so a RunOnce that ignores cancellation delays
	// shutdown, and lease release, for every job, not just its own.
	RunOnce(ctx context.Context) error
}

// Runner ticks jobs on their intervals while it holds their leases.
type Runner struct {
	leaser store.Leaser
	holder string
	ttl    time.Duration
	log    *slog.Logger
	mu     sync.Mutex
	jobs   []Job
	held   map[string]bool
}

// NewRunner creates a runner. leaseTTL must exceed the longest job interval.
func NewRunner(leaser store.Leaser, holder string, leaseTTL time.Duration, log *slog.Logger) *Runner {
	return &Runner{leaser: leaser, holder: holder, ttl: leaseTTL, log: log.With("component", "jobs"), held: map[string]bool{}}
}

// Add registers a job before Run. It returns an error if j.Name() is empty or
// j.Interval() is not positive; Run also defensively skips (and logs) any job
// that slips past this check with a non-positive interval, rather than
// spinning or panicking.
func (r *Runner) Add(j Job) error {
	if j.Name() == "" {
		return errors.New("jobs: job name must not be empty")
	}
	if j.Interval() <= 0 {
		return fmt.Errorf("jobs: job %q interval must be > 0, got %s", j.Name(), j.Interval())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs = append(r.jobs, j)
	return nil
}

func leaseName(name string) string { return "jobs/" + name }

// Tick acquires or renews the job's lease and, if held, runs it once. It returns
// whether the job ran and the job's error. Tick must not be called concurrently
// for the same job from outside Run's own sequencing: Run only ever has one
// Tick in flight per job, waiting for it to return before scheduling the next.
func (r *Runner) Tick(ctx context.Context, j Job) (bool, error) {
	until, err := r.leaser.TryAcquireOrRenew(ctx, leaseName(j.Name()), r.holder, r.ttl)
	if err != nil {
		return false, err
	}
	held := !until.IsZero()
	r.mu.Lock()
	r.held[j.Name()] = held
	r.mu.Unlock()
	if !held {
		jobLeaseHeld.WithLabelValues(j.Name()).Set(0)
		return false, nil
	}
	jobLeaseHeld.WithLabelValues(j.Name()).Set(1)
	return true, j.RunOnce(ctx)
}

// ReleaseAll releases every lease this runner holds (including leases taken via
// Tick for jobs that were never Added), joining every release error rather
// than stopping at the first. Like Tick, it must not be called concurrently
// with an in-flight Tick for the same job outside Run's own wait-then-release
// sequencing: Run waits for every job goroutine to exit before calling
// ReleaseAll.
func (r *Runner) ReleaseAll(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for name, held := range r.held {
		if !held {
			continue
		}
		if err := r.leaser.ReleaseLease(ctx, leaseName(name), r.holder); err != nil {
			errs = append(errs, err)
		}
		r.held[name] = false
		jobLeaseHeld.WithLabelValues(name).Set(0)
	}
	return errors.Join(errs...)
}

// Run ticks every job on its interval until ctx is done, then releases leases.
// Each job's next tick is scheduled from the completion of its previous Tick
// (not on a fixed grid), so a RunOnce slower than the job's interval never
// runs back-to-back with the next.
func (r *Runner) Run(ctx context.Context) error {
	r.mu.Lock()
	jobs := append([]Job(nil), r.jobs...)
	r.mu.Unlock()
	var wg sync.WaitGroup
	for _, j := range jobs {
		if j.Interval() <= 0 {
			r.log.Warn("skipping job with non-positive interval", "job", j.Name(), "interval", j.Interval())
			continue
		}
		wg.Add(1)
		go func(j Job) {
			defer wg.Done()
			// Fires immediately for the first tick; Reset below replaces it from
			// each Tick's completion.
			timer := time.NewTimer(0)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}
				if _, err := r.Tick(ctx, j); err != nil && ctx.Err() == nil {
					r.log.Warn("job failed", "job", j.Name(), "err", err)
				}
				timer.Reset(j.Interval())
			}
		}(j)
	}
	wg.Wait()
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return r.ReleaseAll(rctx)
}
