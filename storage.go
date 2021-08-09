package gobulk

import (
	"context"
	"fmt"

	"github.com/go-playground/validator"
	"go.uber.org/zap"
)

// initStorage sets the storage base properties, validates it and sets it up.
func initStorage(storage Storage, ctx context.Context, processID string, logger *zap.Logger) error {
	if err := storage.Prepare(ctx, processID, logger); err != nil {
		return err
	}
	if err := validator.New().Struct(storage); err != nil {
		return fmt.Errorf("storage validation error: %v", err)
	}
	if err := storage.Setup(); err != nil {
		return fmt.Errorf("storage setup error: %v", err)
	}
	return nil
}

// Storage is the base interface for all inputs, outputs and trackers.
type Storage interface {
	// Prepare validates the config and sets the base properties.
	Prepare(ctx context.Context, processID string, logger *zap.Logger) error
	// Setup contains the storage preparations like connection etc. Is called only once at the very
	// beginning of the work with the storage.
	Setup() error
	// BeforeRun is called right before each process cycle in order to prepare the storage for the
	// next run.
	BeforeRun() error
	// AfterRun is called right after each process cycle in order to finalize the storage operations.
	AfterRun() error
	// Shutdown is called only once at the very end of the work with the storage. It is meant to
	// perform cleanups, close connections and so on.
	Shutdown()
}

// BaseStorage contains base fields and methods for all inputs, outputs and trackers. It is a base
// for them and it must be embedded into them.
type BaseStorage struct {
	ProcessID string `validate:"required"`
	Context   context.Context
	Logger    *zap.Logger `validate:"required"`
}

// Prepare sets the storage base properties.
func (b *BaseStorage) Prepare(ctx context.Context, processID string, logger *zap.Logger) error {
	b.ProcessID = processID
	b.Context = ctx
	b.Logger = logger
	return nil
}

// BeforeRun is called right before each process cycle in order to prepare the storage for the
// next run. As for the BaseStorage, the method does nothing. It can be redefined in the concrete
// storage to set the behaviour.
func (b *BaseStorage) BeforeRun() error { return nil }

// AfterRun is called right after each process cycle in order to finalize the storage operations.
// As for the BaseStorage, the method does nothing. It can be redefined in the concrete storage to
// set the behaviour.
func (b *BaseStorage) AfterRun() error { return nil }

// Shutdown is called only once at the very end of the work with the storage. It is meant to perform
// cleanups, close connections and so on. As for the BaseStorage, the method does nothing. It can be
// redefined in the concrete storage to set the behaviour.
func (b *BaseStorage) Shutdown() {}
