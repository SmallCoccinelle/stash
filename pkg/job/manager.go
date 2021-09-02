package job

import (
	"context"
	"sync"
	"time"

	"github.com/stashapp/stash/pkg/utils"
)

const maxGraveyardSize = 10
const defaultThrottleLimit = time.Second

// Manager maintains a queue of jobs. Jobs are executed one at a time.
type Manager struct {
	queue     []*Job
	graveyard []*Job

	mutex     sync.Mutex
	pending   chan *Job // Pending jobs for the manager
	completed chan *Job // Jobs which are completed
	canceled  chan int  // Cancel requests. Sending -1 cancels all jobs

	lastID              int
	subscriptions       []*ManagerSubscription
	updateThrottleLimit time.Duration
}

// NewManager initialises and returns a new Manager.
func NewManager(ctx context.Context) *Manager {
	ret := &Manager{
		pending:             make(chan *Job),
		completed:           make(chan *Job),
		canceled:            make(chan int),
		updateThrottleLimit: defaultThrottleLimit,
	}

	go ret.dispatcher(ctx)

	return ret
}

// Add queues a job.
func (m *Manager) Add(ctx context.Context, description string, e JobExec) int {
	return m.add(ctx, description, e, false)
}

// Start adds a job and starts it immediately, concurrently with any other
// jobs.
func (m *Manager) Start(ctx context.Context, description string, e JobExec) int {
	return m.add(ctx, description, e, true)
}

func (m *Manager) add(ctx context.Context, description string, e JobExec, immediate bool) int {
	t := time.Now()

	ID := m.nextID()
	j := Job{
		ID:          ID,
		Status:      StatusReady,
		Immediate:   immediate,
		Description: description,
		AddTime:     t,
		exec:        e,
		outerCtx:    ctx,
	}

	m.pending <- &j

	return ID
}

func (m *Manager) notifyNewJob(j *Job) {
	// assumes lock held
	for _, s := range m.subscriptions {
		// don't block if channel is full
		select {
		case s.newJob <- *j:
		default:
		}
	}
}

func (m *Manager) nextID() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.lastID++
	return m.lastID
}

// getReadyJob analyzes the queue for job readiness
// returns a ready job, or nil if no such job exist
func (m *Manager) getReadyJob() *Job {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	running := 0
	var firstJob *Job

	// Consider all jobs in the queue
	// Count the running jobs
	// Find the first job which is ready
	for _, j := range m.queue {
		if j.Status == StatusRunning {
			running++
		}

		if j.Status == StatusReady {
			if j.Immediate {
				// Immediate jobs are always ready for dispatching
				return j
			}

			// First ready job found
			if firstJob == nil {
				firstJob = j
			}
		}
	}

	// If there are running jobs, and no immediate jobs available, we are
	// not ready. Otherwise, select the first ready job from the queue
	if running > 0 {
		return nil
	} else {
		return firstJob
	}
}

func (m *Manager) dispatcher(ctx context.Context) {
	for {
		// The invariant for dispatching is that if there's a job
		// which is ready, we should run said job
		if j := m.getReadyJob(); j != nil {
			m.dispatch(j)
			// Might be more than one job ready, consider them all up-front
			continue
		}

		// Handle events which alters the queue/graveyard
		select {
		case j := <-m.pending:
			m.addJob(j)
		case j := <-m.completed:
			m.completeJob(j)
		case id := <-m.canceled:
			m.cancelJob(id)
		case <-ctx.Done():
			// Cancel everything, drain stopped jobs
			for remaining := m.cancelJob(-1); remaining > 0; remaining-- {
				<-m.completed
			}
			return
		}
	}
}

func (m *Manager) addJob(j *Job) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.queue = append(m.queue, j)
	m.notifyNewJob(j)
}

func (m *Manager) newProgress(j *Job) *Progress {
	return &Progress{
		updater: &updater{
			m:   m,
			job: j,
		},
		percent: ProgressIndefinite,
	}
}

func (m *Manager) dispatch(j *Job) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	t := time.Now()
	j.StartTime = &t
	j.Status = StatusRunning

	ctx, cancelFunc := context.WithCancel(utils.ValueOnlyContext(j.outerCtx))
	j.cancelFunc = cancelFunc

	go func() {
		progress := m.newProgress(j)
		j.exec.Execute(ctx, progress)

		m.completed <- j
	}()

	m.notifyJobUpdate(j)
}

func (m *Manager) addGraveyard(job *Job) {
	m.graveyard = append(m.graveyard, job)
	if len(m.graveyard) > maxGraveyardSize {
		m.graveyard = m.graveyard[1:]
	}
}

// removeJob moves a job into the graveyard
func (m *Manager) removeJob(index int, job *Job) {
	// Assumes lock is held
	m.queue = append(m.queue[:index], m.queue[index+1:]...)

	m.addGraveyard(job)

	// notify job removed
	for _, s := range m.subscriptions {
		// don't block if channel is full
		select {
		case s.removedJob <- *job:
		default:
		}
	}
}

func (m *Manager) completeJob(job *Job) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	index, _ := m.getJob(m.queue, job.ID)
	if index == -1 {
		return
	}

	// Mark job as completed, move it to the graveyard
	job.complete()
	m.removeJob(index, job)

}

func (m *Manager) getJob(list []*Job, id int) (index int, job *Job) {
	// assumes lock held
	for i, j := range list {
		if j.ID == id {
			index = i
			job = j
			return
		}
	}

	return -1, nil
}

// CancelJob cancels the job with the provided id. Jobs that have been started
// are notified that they are stopping. Jobs that have not yet started are
// removed from the queue. If no job exists with the provided id, then there is
// no effect. Likewise, if the job is already cancelled, there is no effect.
func (m *Manager) CancelJob(id int) {
	m.canceled <- id
}

// CancelAll cancels all of the jobs in the queue. This is the same as
// calling CancelJob on all jobs in the queue.
func (m *Manager) CancelAll() {
	m.canceled <- -1
}

// cancelJob cancels the job with the given id. It returns the number of
// jobs which are in the stopping state and will complete.
func (m *Manager) cancelJob(id int) int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	stopCount := 0

	// Cancel all jobs
	if id == -1 {
		// Go through the queue, and cancel jobs.
		// - The jobs which are stopping will complete on their own
		// - The jobs which are canceled are removed from the queue
		//   and added to the graveyard.
		var filteredQueue []*Job
		for _, j := range m.queue {
			j.cancel()
			if j.Status == StatusStopping {
				filteredQueue = append(filteredQueue, j)
				stopCount++
			} else {
				m.addGraveyard(j)

				// notify job removed
				for _, s := range m.subscriptions {
					// don't block if channel is full
					select {
					case s.removedJob <- *j:
					default:
					}
				}
			}
		}
		m.queue = filteredQueue
		return stopCount
	}

	// Cancel job with given id
	i, j := m.getJob(m.queue, id)
	if j == nil {
		return 0
	}

	j.cancel()
	if j.Status == StatusCancelled {
		m.removeJob(i, j)
	} else if j.Status == StatusStopping {
		return 1
	}

	return 0
}

// GetJob returns a copy of the Job for the provided id. Returns nil if the job
// does not exist.
func (m *Manager) GetJob(id int) *Job {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// get from the queue or graveyard
	_, j := m.getJob(append(m.queue, m.graveyard...), id)
	if j != nil {
		// make a copy of the job and return the pointer
		jCopy := *j
		return &jCopy
	}

	return nil
}

// GetQueue returns a copy of the current job queue.
func (m *Manager) GetQueue() []Job {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var ret []Job

	for _, j := range m.queue {
		jCopy := *j
		ret = append(ret, jCopy)
	}

	return ret
}

// Subscribe subscribes to changes to jobs in the manager queue.
func (m *Manager) Subscribe(ctx context.Context) *ManagerSubscription {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	ret := newSubscription()

	m.subscriptions = append(m.subscriptions, ret)

	go func() {
		<-ctx.Done()
		m.mutex.Lock()
		defer m.mutex.Unlock()

		ret.close()

		// remove from the list
		for i, s := range m.subscriptions {
			if s == ret {
				m.subscriptions = append(m.subscriptions[:i], m.subscriptions[i+1:]...)
				break
			}
		}
	}()

	return ret
}

func (m *Manager) notifyJobUpdate(j *Job) {
	// don't update if job is finished or cancelled - these are handled
	// by removeJob
	if j.Status == StatusCancelled || j.Status == StatusFinished {
		return
	}

	// assumes lock held
	for _, s := range m.subscriptions {
		// don't block if channel is full
		select {
		case s.updatedJob <- *j:
		default:
		}
	}
}

type updater struct {
	m           *Manager
	job         *Job
	lastUpdate  time.Time
	updateTimer *time.Timer
}

func (u *updater) notifyUpdate() {
	// assumes lock held
	u.m.notifyJobUpdate(u.job)
	u.lastUpdate = time.Now()
	u.updateTimer = nil
}

func (u *updater) updateProgress(progress float64, details []string) {
	u.m.mutex.Lock()
	defer u.m.mutex.Unlock()

	u.job.Progress = progress
	u.job.Details = details

	if time.Since(u.lastUpdate) < u.m.updateThrottleLimit {
		if u.updateTimer == nil {
			u.updateTimer = time.AfterFunc(u.m.updateThrottleLimit-time.Since(u.lastUpdate), func() {
				u.m.mutex.Lock()
				defer u.m.mutex.Unlock()

				u.notifyUpdate()
			})
		}
	} else {
		u.notifyUpdate()
	}
}
