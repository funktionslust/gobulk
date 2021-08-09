package gobulk

import (
	"context"
)

// Input represents a storage that contains the data to be imported. The Input interface is used to
// fetch containers that have been prepared for being imported.
type Input interface {
	Storage
	// Scan scans the storage for new containers starting after marker and sends them to the containers
	// channel. If the marker is nil, the whole storage is scanned. In the end of the scan, once all
	// the available containers have been scanned, input should notify the containers channel listener
	// about it by sending a value to the done channel. If a scan error occurres, it must be sent to
	// the errors channel. A sent error indicates stopped and failed scanning.
	Scan(ctx context.Context, marker *Container, contCh chan<- []*Container, doneCh chan<- struct{}, errCh chan<- error)
	// Read reads the raw data of a container and returns paths+filenames (i.e. repositories+identifiers)
	// mapped to the data of the files.
	// It can happen that one needs to read/plan/import multiple files in a single run because their
	// contents have cross-dependencies, and in this scenario a container stands for a sub-repository
	// (a folder containing files), not a single file.
	Read(container *Container) (map[string][]byte, error)
}
