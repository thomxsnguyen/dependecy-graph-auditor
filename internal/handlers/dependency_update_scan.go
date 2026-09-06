package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	githubsource "github.com/thomxsnguyen/mini-distributed-job-api/internal/github"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/job"
)

type DependencyUpdateScanHandler struct{ GitHub ManifestFetcher }

func (h DependencyUpdateScanHandler) Handle(ctx context.Context, value job.Job) (job.HandlerResult, error) {
	var payload DependencyAuditPayload
	if err := json.Unmarshal(value.Payload, &payload); err != nil {
		return job.HandlerResult{}, job.Failure(job.ErrorPermanent, fmt.Errorf("invalid dependency update scan payload: %w", err))
	}
	repository, err := githubsource.ParseRepositoryURL(strings.TrimSpace(payload.RepositoryURL))
	if err != nil {
		return job.HandlerResult{}, job.Failure(job.ErrorPermanent, err)
	}
	manifests, err := loadRepositoryManifests(ctx, h.GitHub, repository, payload.Ref)
	if err != nil {
		return job.HandlerResult{}, err
	}
	children := []job.Submission{}
	seen := make(map[string]bool)
	for _, entry := range manifests {
		for _, dependency := range entry.manifest.Dependencies {
			if dependency.Indirect {
				continue
			}
			key := dependencyChildKey("update-child:", value.RootJobID, entry.ecosystem, dependency.Name, dependency.VersionRange)
			if seen[key] {
				continue
			}
			seen[key] = true
			body, err := json.Marshal(updateCheckPayload{Ecosystem: entry.ecosystem, Name: dependency.Name, VersionRange: dependency.VersionRange})
			if err != nil {
				return job.HandlerResult{}, job.Failure(job.ErrorPermanent, err)
			}
			children = append(children, job.Submission{Type: "package_update_check", Payload: body,
				MaxAttempts: value.MaxAttempts, RootJobID: value.RootJobID, ParentJobID: value.ID,
				Internal: true, IdempotencyKey: key})
		}
	}
	result, err := json.Marshal(map[string]any{"repositoryUrl": payload.RepositoryURL, "ref": payload.Ref, "dependencyCount": len(children), "auditId": value.RootJobID})
	if err != nil {
		return job.HandlerResult{}, job.Failure(job.ErrorPermanent, err)
	}
	return job.HandlerResult{Result: result, Children: children}, nil
}
