package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/thomxsnguyen/mini-distributed-job-api/internal/auditor"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/gomod"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/job"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/updater"
)

type GoLatestClient interface {
	LatestVersion(context.Context, string) (string, error)
}

type UpdateCheckRegistries struct {
	NPM  auditor.RegistryClient
	PyPI auditor.RegistryClient
	Go   GoLatestClient
}

type PackageUpdateCheckHandler struct{ Registries UpdateCheckRegistries }

type updateCheckPayload struct {
	Ecosystem    string `json:"ecosystem"`
	Name         string `json:"name"`
	VersionRange string `json:"versionRange"`
}

type updateCheckResult struct {
	Ecosystem      string            `json:"ecosystem"`
	Name           string            `json:"name"`
	CurrentVersion string            `json:"currentVersion"`
	LatestVersion  string            `json:"latestVersion"`
	Staleness      updater.Staleness `json:"staleness"`
}

func (h PackageUpdateCheckHandler) Handle(ctx context.Context, value job.Job) (job.HandlerResult, error) {
	var payload updateCheckPayload
	if err := json.Unmarshal(value.Payload, &payload); err != nil {
		return job.HandlerResult{}, job.Failure(job.ErrorPermanent, fmt.Errorf("invalid package update check payload: %w", err))
	}
	if strings.TrimSpace(payload.Name) == "" {
		return job.HandlerResult{}, job.Failure(job.ErrorPermanent, fmt.Errorf("package update check requires a name"))
	}
	var current, latest string
	switch payload.Ecosystem {
	case "npm", "pypi":
		registry := h.Registries.NPM
		if payload.Ecosystem == "pypi" {
			registry = h.Registries.PyPI
		}
		if registry == nil {
			return job.HandlerResult{}, job.Failure(job.ErrorPermanent, fmt.Errorf("missing %s registry", payload.Ecosystem))
		}
		if payload.Ecosystem == "npm" && strings.TrimSpace(payload.VersionRange) == "" {
			return job.HandlerResult{}, job.Failure(job.ErrorPermanent, fmt.Errorf("npm update check requires a version range"))
		}
		meta, err := registry.FetchPackage(ctx, payload.Name, payload.VersionRange)
		if err != nil {
			return job.HandlerResult{}, classifyError(err)
		}
		current = meta.Version
		latest, err = registry.LatestVersion(ctx, payload.Name)
		if err != nil {
			return job.HandlerResult{}, classifyError(err)
		}
	case "go":
		if err := gomod.ValidateCoordinate(payload.Name, payload.VersionRange); err != nil {
			return job.HandlerResult{}, job.Failure(job.ErrorPermanent, err)
		}
		if h.Registries.Go == nil {
			return job.HandlerResult{}, job.Failure(job.ErrorPermanent, fmt.Errorf("missing Go registry"))
		}
		current = payload.VersionRange
		var err error
		latest, err = h.Registries.Go.LatestVersion(ctx, payload.Name)
		if err != nil {
			var classified interface{ Retryable() bool }
			if errors.As(err, &classified) && classified.Retryable() {
				return job.HandlerResult{}, job.Failure(job.ErrorTransient, err)
			}
			return job.HandlerResult{}, job.Failure(job.ErrorPermanent, err)
		}
	default:
		return job.HandlerResult{}, job.Failure(job.ErrorPermanent, fmt.Errorf("unsupported update ecosystem %q", payload.Ecosystem))
	}
	result, err := json.Marshal(updateCheckResult{Ecosystem: payload.Ecosystem, Name: payload.Name,
		CurrentVersion: current, LatestVersion: latest, Staleness: updater.Classify(current, latest)})
	if err != nil {
		return job.HandlerResult{}, job.Failure(job.ErrorPermanent, err)
	}
	return job.HandlerResult{Result: result}, nil
}
