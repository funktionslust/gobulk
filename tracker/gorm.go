package tracker

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/funktionslust/gobulk"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

const (
	// trackOperationsChunkSize is the max amount of track container operations performed at a time.
	trackOperationsChunkSize = 5000
)

// GORMTrackerConfig represents the GORMTracker config structure.
type GORMTrackerConfig struct {
	Host               string           `validate:"required"`
	Database           string           `validate:"required"`
	User               string           `validate:"required"`
	Password           string           `validate:"required"`
	Port               string           `validate:"required"`
	Logger             logger.Interface `validate:"required"`
	CleanupOnStart     bool
	CleanupBetweenRuns bool
}

// NewGORMTracker returns a new instance of the GORMTracker.
func NewGORMTracker(cfg GORMTrackerConfig) *GORMTracker {
	return &GORMTracker{
		Cfg: cfg,
	}
}

// GORMTracker represents a tracker that stores the import progress inside a database supported by
// gorm like PostgresSQL, MySQL and others.
type GORMTracker struct {
	gobulk.BaseStorage
	Cfg       GORMTrackerConfig
	client    *gorm.DB
	iteration *gobulk.Iteration
	dirty     bool
}

// BeforeRun is called right before each process cycle in order to prepare the storage for the run.
// As for the GORMTracker, it's possible to configure the tracker to resets started but not finished
// containers between runs by setting t.CleanupBetweenRuns to true.
func (t *GORMTracker) BeforeRun() error {
	if t.Cfg.CleanupBetweenRuns && t.dirty {
		return t.cleanup()
	}
	return nil
}

// AfterRun is called right after each process cycle in order to finalize the storage operations.
// As for the GORMTracker, it simply marks the tracker as "dirty".
func (t *GORMTracker) AfterRun() error {
	t.dirty = true
	return nil
}

// Shutdown is called only once at the very end of the work with the storage. As for the GORMTracker,
// it closes the initially opened db connection.
func (t *GORMTracker) Shutdown() {
	db, _ := t.client.DB()
	if db != nil {
		db.Close()
	}
}

// CurrentIteration retrieves the current iteration state of the passed format.
func (t *GORMTracker) CurrentIteration(format gobulk.Format) (*gobulk.Iteration, error) {
	if t.iteration == nil {
		i := &iteration{}
		if err := t.client.Preload(clause.Associations).Model(&iteration{}).Order("number desc").Take(i, "format = ?", format.Name()).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil, nil
			}
			return nil, err
		}
		t.configIteration(format, i)
	}
	return t.iteration, nil
}

// NewIteration creates a new iteration based on the format definitions and saves it in the tracker.
func (t *GORMTracker) NewIteration(format gobulk.Format, number uint) (*gobulk.Iteration, error) {
	formatName := format.Name()
	i := &iteration{Format: &formatName, Number: &number}
	if err := t.client.Create(i).Error; err != nil {
		return nil, err
	}
	t.configIteration(format, i)
	return t.iteration, nil
}

// GetUnfinishedContainers returns a list of containers which have already been tracked but haven't
// yet been finished.
func (t *GORMTracker) GetUnfinishedContainers() ([]*gobulk.Container, error) {
	var containers []container
	if err := t.client.Preload(clause.Associations).Find(&containers, "started IS NULL AND finished IS NULL AND iteration_id = ? AND process_id = ?", t.iteration.ID, t.ProcessID).Error; err != nil {
		return nil, err
	}
	return t.convertContainers(containers), nil
}

// TrackContainers tracks the containers in the slice and updates the corresponding Iteration last
// tracked container with the last one in the slice.
func (t *GORMTracker) TrackContainers(containers []*gobulk.Container) (*gobulk.TrackContainersResponse, error) {
	resp := &gobulk.TrackContainersResponse{
		Tracked:    make([]*gobulk.Container, 0, len(containers)),
		Conflicted: make([]*gobulk.Container, 0, len(containers)),
	}
	for _, c := range containers {
		dbcontainer := convertDBContainer(c)
		tx := t.client.Clauses(clause.OnConflict{DoNothing: true}).Create(&dbcontainer)
		if tx.Error != nil {
			return nil, tx.Error
		}
		c.ID = uint64(dbcontainer.ID)
		if tx.RowsAffected == 0 {
			resp.Conflicted = append(resp.Conflicted, c)
		} else {
			resp.Tracked = append(resp.Tracked, c)
		}
	}
	if len(resp.Tracked) != 0 {
		if err := t.updateLastTrackedContainer(t.getMaxIDContainer(resp.Tracked)); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

// NextContainers by default, searches for and returns new processable containers and locks them
// (marks as started). However, it's possible to modify the method behaviour by the opts parameter.
func (t *GORMTracker) NextContainers(readStrategy gobulk.Strategy, number int, opts ...gobulk.TrackerNextContainersOpt) ([]*gobulk.Container, error) {
	order := "ASC"
	if readStrategy == gobulk.StrategyLIFO {
		order = "DESC"
	}
	trackerOpts := newTrackerOpts(opts)
	query, err := t.buildGetNextContainersQuery(trackerOpts.shouldGetOnlyNew)
	if err != nil {
		return nil, err
	}
	var containers []container
	if err := t.client.Preload(clause.Associations).Order("created_at, id "+order).Limit(number).Find(&containers, query...).Error; err != nil && err != gorm.ErrRecordNotFound {
		return nil, err
	}
	if len(containers) == 0 {
		return nil, nil
	}
	if trackerOpts.shouldLockContainers {
		if err := t.lockContainers(containers); err != nil {
			return nil, err
		}
	}
	return t.convertContainers(containers), nil
}

// TrackContainerOperations persists the containers operations and their error/success status.
func (t *GORMTracker) TrackContainerOperations(containers []*gobulk.Container) (*gobulk.ProcessContainersResult, error) {
	result := gobulk.NewProcessContainersResult(nil, nil)
	chunk := newTrackOperationsChuck()
	succeededContainers := make(map[*gobulk.Container]struct{})
containers_loop:
	for _, container := range containers {
		containerOps := container.GetAllOperations()
		if len(containerOps) == 0 {
			succeededContainers[container] = struct{}{}
			continue
		}
		for _, o := range containerOps {
			chunk.RegisterContainer(container)
			chunk.AddOperation(o)
			if chunk.IsComplete() {
				if err := t.client.Create(chunk.operations).Error; err != nil {
					t.handleFailedOperations(chunk.gobulkOperations, result, err, gobulk.IssueTypePersistance)
					chunk.Reset()
					continue containers_loop
				}
				chunk.MapOperationIDs()
				for c := range chunk.containers {
					succeededContainers[c] = struct{}{}
				}
				chunk.Reset()
			}
		}
	}
	if !chunk.IsEmpty() {
		if err := t.client.Create(chunk.operations).Error; err != nil {
			t.handleFailedOperations(chunk.gobulkOperations, result, err, gobulk.IssueTypePersistance)
		} else {
			chunk.MapOperationIDs()
			for c := range chunk.containers {
				succeededContainers[c] = struct{}{}
			}
		}
	}
	result.Succeeded = append(result.Succeeded, containersMapToSlice(succeededContainers)...)
	return result, nil
}

// FinishContainers sets the containers state as successfully and completely imported.
func (t *GORMTracker) FinishContainers(containers []*gobulk.Container) (*gobulk.ProcessContainersResult, error) {
	result := gobulk.NewProcessContainersResult(nil, nil)
	if len(containers) == 0 {
		return result, nil
	}
	now := time.Now()
	bulkFinishContainers := make([]*gobulk.Container, 0, len(containers))
	for _, c := range containers {
		if c.Note != "" {
			if err := t.finishContainerWithNote(c, now); err != nil {
				result.Failed.Append(c, gobulk.NewIssue(err, "", gobulk.IssueTypePersistance, ""))
			} else {
				result.Succeeded = append(result.Succeeded, c)
			}
		} else {
			bulkFinishContainers = append(bulkFinishContainers, c)
		}
	}
	if len(bulkFinishContainers) != 0 {
		if err := t.bulkFinishContainers(bulkFinishContainers, now); err != nil {
			result.FailContainers(bulkFinishContainers, err, "", "", gobulk.IssueTypePersistance)
		} else {
			result.Succeeded = append(result.Succeeded, bulkFinishContainers...)
		}
	}
	if len(result.Succeeded) != 0 {
		if err := t.updateLastProcessedContainer(t.getMaxIDContainer(result.Succeeded)); err != nil {
			result = gobulk.NewProcessContainersResult(nil, nil)
			result.FailContainers(containers, err, "", "", gobulk.IssueTypePersistance)
			return result, nil
		}
	}
	return result, nil
}

// TrackIssue tracks the issue.
func (t *GORMTracker) TrackIssue(issue *gobulk.Issue) error {
	dbissue := convertDBIssue(issue)
	if err := t.client.Create(&dbissue).Error; err != nil {
		return err
	}
	return nil
}

// cleanup resets the started field of the started but not finished containers.
func (t *GORMTracker) cleanup() error {
	return t.client.Exec("UPDATE containers SET started = null WHERE created_at IS NOT NULL AND started IS NOT NULL and finished IS NULL AND process_id = ?", t.ProcessID).Error
}

// configIteration configures the tracker gobulk.Iteration using the format and the iteration model.
func (t *GORMTracker) configIteration(f gobulk.Format, i *iteration) {
	t.iteration = i.Convert()
	t.iteration.Format = f
	t.iteration.Tracker = f.Tracker()
	t.iteration.Input = f.Input()
	t.iteration.Output = f.Output()
}

// buildGetNextContainersQuery returns a query to get next containers. If onlyNew is true, the result
// query will return unfinished containers after the last processed one.
func (t *GORMTracker) buildGetNextContainersQuery(onlyNew bool) ([]interface{}, error) {
	if onlyNew {
		var lastProcessedContainer container
		err := t.client.Order("id DESC").First(&lastProcessedContainer, "finished IS NOT NULL AND iteration_id = ? AND process_id = ?", t.iteration.ID, t.ProcessID).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return nil, err
		}
		if lastProcessedContainer.ID != 0 {
			return []interface{}{
				"id > ? AND started IS NULL AND finished IS NULL AND iteration_id = ? AND process_id = ?",
				lastProcessedContainer.ID, t.iteration.ID, t.ProcessID,
			}, nil
		}
	}
	return []interface{}{
		"started IS NULL AND finished IS NULL AND iteration_id = ? AND process_id = ?",
		t.iteration.ID, t.ProcessID,
	}, nil
}

// lockContainers marks the passed containers as started.
func (t *GORMTracker) lockContainers(containers []container) error {
	ids := make([]uint64, 0, len(containers))
	for _, c := range containers {
		ids = append(ids, uint64(c.ID))
	}
	return t.client.Model(&container{}).Where("id IN (?)", ids).Update("started", time.Now()).Error
}

// convertContainers converts db containers to gobulk containers.
func (t *GORMTracker) convertContainers(containers []container) []*gobulk.Container {
	converted := []*gobulk.Container{}
	for _, c := range containers {
		converted = append(converted, c.Convert())
	}
	return converted
}

// handleFailedOperations populates the res issues list with issues created for the passed operations.
func (t *GORMTracker) handleFailedOperations(ops []*gobulk.Operation, res *gobulk.ProcessContainersResult, err error, iType gobulk.IssueType) {
	for _, op := range ops {
		res.Failed.Append(op.Container, gobulk.NewIssue(err, "", iType, ""))
	}
}

// finishContainerWithNote marks the container as finished with the finishedAt time and populates
// the container note field with the c.Note value.
func (t *GORMTracker) finishContainerWithNote(c *gobulk.Container, finishedAt time.Time) error {
	if err := t.client.Model(&container{}).Where("id = ?", c.ID).Updates(map[string]interface{}{
		"finished": finishedAt,
		"note":     c.Note,
	}).Error; err != nil {
		return fmt.Errorf("failed to finish a container: %v", err)
	}
	return nil
}

// bulkFinishContainers marks the containers as finished with the finishedAt time.
func (t *GORMTracker) bulkFinishContainers(containers []*gobulk.Container, finishedAt time.Time) error {
	containerIDs := make([]uint64, 0, len(containers))
	for _, c := range containers {
		containerIDs = append(containerIDs, c.ID)
	}
	if err := t.client.Model(&container{}).Where("id IN (?)", containerIDs).Update("finished", finishedAt).Error; err != nil {
		return fmt.Errorf("failed to finish containers bulk: %v", err)
	}
	return nil
}

// updateLastProcessedContainer updates the iteration last_processed_container_id field with the
// container id.
func (t *GORMTracker) updateLastProcessedContainer(container *gobulk.Container) error {
	if err := t.client.Model(&iteration{}).Where("id = ?", t.iteration.ID).Updates(map[string]interface{}{
		"last_processed_container_id": container.ID,
	}).Error; err != nil {
		return fmt.Errorf("failed to update iteration last processed container: %v", err)
	}
	t.iteration.LastProcessedContainer = container
	return nil
}

// updateLastTrackedContainer updates the iteration last_tracked_container_id field with the
// container id.
func (t *GORMTracker) updateLastTrackedContainer(container *gobulk.Container) error {
	if err := t.client.Model(&iteration{}).Where("id = ?", t.iteration.ID).Updates(map[string]interface{}{
		"last_tracked_container_id": container.ID,
	}).Error; err != nil {
		return fmt.Errorf("failed to update iteration last tracked container: %v", err)
	}
	t.iteration.LastTrackedContainer = container
	return nil
}

// getMaxIDContainer returns a container with the max ID from the passed slice of containers.
func (GORMTracker) getMaxIDContainer(containers []*gobulk.Container) *gobulk.Container {
	var max *gobulk.Container
	for _, container := range containers {
		if max == nil || container.ID > max.ID {
			max = container
		}
	}
	return max
}

// newTrackerOpts transforms a slice of gobulk tracker option parameters to a new instance of
// trackerOpts.
func newTrackerOpts(opts []gobulk.TrackerNextContainersOpt) trackerOpts {
	t := trackerOpts{
		shouldLockContainers: true,
		shouldGetOnlyNew:     false,
	}
	for _, opt := range opts {
		switch opt {
		case gobulk.TrackerNextContainersOptNoLock:
			t.shouldLockContainers = false
		case gobulk.TrackerNextContainersOptOnlyNew:
			t.shouldGetOnlyNew = true
		}
	}
	return t
}

// containersMapToSlice converts the containers map to containers slice.
func containersMapToSlice(m map[*gobulk.Container]struct{}) []*gobulk.Container {
	sl := make([]*gobulk.Container, 0, len(m))
	for c := range m {
		sl = append(sl, c)
	}
	return sl
}

// trackerOpts represents joined into a struct tracker options.
type trackerOpts struct {
	shouldLockContainers bool
	shouldGetOnlyNew     bool
}

// iteration is a model of gobulk.Iteration saved in the tracker.
type iteration struct {
	gorm.Model
	Number                   *uint `gorm:"index;not null;default:1"`
	LastTrackedContainerID   *uint64
	LastTrackedContainer     *container `gorm:"foreignkey:LastTrackedContainerID;"`
	LastProcessedContainerID *uint64
	LastProcessedContainer   *container `gorm:"foreignkey:LastProcessedContainerID"`
	Format                   *string    `gorm:"index;not null" sql:"type:VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
}

// Convert converts the iteration to a gobulk.Iteration.
func (i *iteration) Convert() *gobulk.Iteration {
	var lastTrackedContainer *gobulk.Container
	if i.LastTrackedContainer != nil {
		lastTrackedContainer = i.LastTrackedContainer.Convert()
	}
	var lastProcessedContainer *gobulk.Container
	if i.LastProcessedContainer != nil {
		lastProcessedContainer = i.LastProcessedContainer.Convert()
	}
	return &gobulk.Iteration{
		ID:                     uint64(i.ID),
		Number:                 *i.Number,
		LastTrackedContainer:   lastTrackedContainer,
		LastProcessedContainer: lastProcessedContainer,
		Created:                i.CreatedAt,
		Modified:               &i.UpdatedAt,
	}
}

// container is a model of gobulk.Container saved in the tracker.
type container struct {
	gorm.Model
	IterationID     *uint64    `gorm:"uniqueIndex:upsert_index;not null"`
	Iteration       iteration  `gorm:"foreignkey:IterationID"`
	Started         *time.Time `gorm:"index"`
	Finished        *time.Time `gorm:"index"`
	InputRepository *string    `gorm:"uniqueIndex:upsert_index;index;not null" sql:"type:VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
	InputIdentifier *string    `gorm:"uniqueIndex:upsert_index;index;not null" sql:"type:VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
	Size            uint64
	ContentHash     string  `gorm:"uniqueIndex:upsert_index;default:''" sql:"type:VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
	Note            string  `sql:"type:LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
	ProcessID       *string `gorm:"index;not null" sql:"type:VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
}

// Convert converts the container to a gobulk.Container.
func (c *container) Convert() *gobulk.Container {
	return &gobulk.Container{
		ID:              uint64(c.ID),
		IterationID:     *c.IterationID,
		Created:         c.CreatedAt,
		Started:         c.Started,
		Finished:        c.Finished,
		InputRepository: *c.InputRepository,
		InputIdentifier: *c.InputIdentifier,
		Size:            c.Size,
		LastModified:    c.UpdatedAt,
		ContentHash:     c.ContentHash,
		Note:            c.Note,
		ProcessID:       *c.ProcessID,
	}
}

// convertDBContainer converts a gobulk.Container into a tracker container model.
func convertDBContainer(c *gobulk.Container) container {
	var iterationID *uint64
	if c.IterationID != 0 {
		iterationID = &c.IterationID
	}
	var inputRepository, inputIdentifier, processID *string
	if c.InputRepository != "" {
		inputRepository = &c.InputRepository
	}
	if c.InputIdentifier != "" {
		inputIdentifier = &c.InputIdentifier
	}
	if c.ProcessID != "" {
		processID = &c.ProcessID
	}
	return container{
		Model: gorm.Model{
			ID:        uint(c.ID),
			CreatedAt: c.Created,
			UpdatedAt: c.LastModified,
		},
		IterationID:     iterationID,
		Started:         c.Started,
		Finished:        c.Finished,
		InputRepository: inputRepository,
		InputIdentifier: inputIdentifier,
		Size:            c.Size,
		ContentHash:     c.ContentHash,
		Note:            c.Note,
		ProcessID:       processID,
	}
}

// operation is a model of gobulk.Operation saved in the tracker.
type operation struct {
	gorm.Model
	IterationID      *uint64    `gorm:"not null"`
	Iteration        *iteration `gorm:"foreignkey:IterationID"`
	ContainerID      *uint64    `gorm:"not null"`
	Container        *container `gorm:"foreignkey:ContainerID"`
	Type             string     `gorm:"index;default:'omit'" sql:"type:VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
	OutputRepository *string    `gorm:"index;not null" sql:"type:VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
	OutputIdentifier *string    `gorm:"index;not null" sql:"type:VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
	Success          uint8      `gorm:"index"`
}

// Convert converts the operation to a gobulk.Operation.
func (o *operation) Convert() *gobulk.Operation {
	return &gobulk.Operation{
		ID:               uint64(o.ID),
		Iteration:        o.Iteration.Convert(),
		Container:        o.Container.Convert(),
		Type:             gobulk.OperationType(o.Type),
		OutputRepository: *o.OutputRepository,
		OutputIdentifier: *o.OutputIdentifier,
		Created:          o.CreatedAt,
		Success:          o.Success == 1,
	}
}

// convertDBOperation converts a gobulk.Operation into a tracker operation model.
func convertDBOperation(o *gobulk.Operation) operation {
	var iterationID, containerID *uint64
	if o.Iteration != nil && o.Iteration.ID != 0 {
		iterationID = &o.Iteration.ID
	}
	if o.Container != nil && o.Container.ID != 0 {
		containerID = &o.Container.ID
	}
	var outputRepository, outputIdentifier *string
	if o.OutputRepository != "" {
		outputRepository = &o.OutputRepository
	}
	if o.OutputIdentifier != "" {
		outputIdentifier = &o.OutputIdentifier
	}
	var success uint8
	if o.Success {
		success = 1
	}
	return operation{
		Model: gorm.Model{
			ID:        uint(o.ID),
			CreatedAt: o.Created,
		},
		IterationID:      iterationID,
		ContainerID:      containerID,
		Type:             string(o.Type),
		OutputRepository: outputRepository,
		OutputIdentifier: outputIdentifier,
		Success:          success,
	}
}

// issue is a model of gobulk.Issue saved in the tracker.
type issue struct {
	gorm.Model
	IterationID *uint64   `gorm:"not null"`
	Iteration   iteration `gorm:"foreignkey:IterationID"`
	ContainerID *uint64
	Container   *container `gorm:"foreignkey:ContainerID"`
	OperationID *uint64
	Operation   *operation `gorm:"foreignkey:OperationID"`
	Step        *string    `gorm:"index;not null" sql:"type:VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
	Type        *string    `gorm:"index;not null" sql:"type:VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
	Payload     string     `sql:"type:LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
	Note        string     `sql:"type:LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
	Error       string     `sql:"type:LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"`
	Handled     *time.Time `gorm:"index"`
}

// Convert converts the issue to a gobulk.Issue.
func (i *issue) Convert() *gobulk.Issue {
	return &gobulk.Issue{
		ID:        uint64(i.ID),
		Iteration: i.Iteration.Convert(),
		Container: i.Container.Convert(),
		Operation: i.Operation.Convert(),
		Step:      gobulk.Step(*i.Step),
		Type:      gobulk.IssueType(*i.Type),
		Payload:   i.Payload,
		Note:      i.Note,
		Handled:   i.Handled,
		Created:   i.CreatedAt,
		Err:       errors.New(i.Error),
	}
}

// convertDBIssue converts a gobulk.Issue into a tracker issue model.
func convertDBIssue(i *gobulk.Issue) issue {
	var iterationID, containerID, operationID *uint64
	if i.Iteration != nil && i.Iteration.ID != 0 {
		iterationID = &i.Iteration.ID
	}
	if i.Container != nil && i.Container.ID != 0 {
		containerID = &i.Container.ID
	}
	if i.Operation != nil && i.Operation.ID != 0 {
		operationID = &i.Operation.ID
	}
	var step, issueType *string
	if i.Step.String() != "" {
		auxStep := i.Step.String()
		step = &auxStep
	}
	if i.Type.String() != "" {
		auxType := i.Type.String()
		issueType = &auxType
	}
	return issue{
		Model: gorm.Model{
			ID:        uint(i.ID),
			CreatedAt: i.Created,
		},
		IterationID: iterationID,
		ContainerID: containerID,
		OperationID: operationID,
		Step:        step,
		Type:        issueType,
		Payload:     i.Payload,
		Note:        i.Note,
		Error:       i.Err.Error(),
		Handled:     i.Handled,
	}
}

// newTrackOperationsChuck initialises a new trackOperationsChuck.
func newTrackOperationsChuck() *trackOperationsChuck {
	chunk := &trackOperationsChuck{}
	chunk.Reset()
	return chunk
}

// trackOperationsChuck contains a gorm tracker TrackContainerOperations related fields usabe in a
// single track chunk.
type trackOperationsChuck struct {
	containers       map[*gobulk.Container]struct{}
	operations       []*operation
	gobulkOperations []*gobulk.Operation
	mappedOperations map[*operation]*gobulk.Operation
}

// RegisterContainer registers the container as a chunk-related.
func (c *trackOperationsChuck) RegisterContainer(container *gobulk.Container) {
	c.containers[container] = struct{}{}
}

// AddOperation saves the operation as a chunk-related.
func (c *trackOperationsChuck) AddOperation(operation *gobulk.Operation) {
	op := convertDBOperation(operation)
	c.operations = append(c.operations, &op)
	c.gobulkOperations = append(c.gobulkOperations, operation)
	c.mappedOperations[&op] = operation
}

// Reset resets the chunk to the empty state.
func (c *trackOperationsChuck) Reset() {
	c.containers = make(map[*gobulk.Container]struct{})
	c.operations = make([]*operation, 0, trackOperationsChunkSize)
	c.gobulkOperations = make([]*gobulk.Operation, 0, trackOperationsChunkSize)
	c.mappedOperations = make(map[*operation]*gobulk.Operation)
	runtime.GC()
}

// IsComplete returns true if the chunk operations amount has reached the trackOperationsChunkSize.
func (c *trackOperationsChuck) IsComplete() bool {
	return len(c.operations) >= trackOperationsChunkSize
}

// IsEmpty returns true if there are no operations that have been prepared in the chunk.
func (c *trackOperationsChuck) IsEmpty() bool {
	return len(c.operations) == 0
}

// MapOperationIDs populates the chunk gobulk operations IDs with corresponding db operations IDs.
func (c *trackOperationsChuck) MapOperationIDs() {
	for op, gobulkOp := range c.mappedOperations {
		gobulkOp.ID = uint64(op.ID)
	}
}
