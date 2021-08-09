package gobulk

import (
	"path"

	"go.uber.org/zap"
)

// GetContainerIDs returns a slice of IDs of the passed containers.
func GetContainerIDs(containers []*Container) []uint64 {
	ids := make([]uint64, 0, len(containers))
	for _, c := range containers {
		ids = append(ids, c.ID)
	}
	return ids
}

// buildContainerKey joins the container identifiers in order to get unique key for it.
func buildContainerKey(c *Container) string {
	return path.Join(c.InputRepository, c.InputIdentifier)
}

// logStepResults uses logger to notify about step results.
func logStepResults(logger *zap.Logger, step string, result *ProcessContainersResult) {
	if len(result.Failed) != 0 {
		logger.Info(step+" end with issues",
			zap.Int(step+"_containers", len(result.Succeeded)),
			zap.Int("failed_containers", len(result.Failed)),
		)
	} else {
		logger.Info(step+" end", zap.Int(step+"_containers", len(result.Succeeded)))
	}
}
