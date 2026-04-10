package server

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/sathish/bigquery-emulator/server/apierror"
)

// listProjects handles GET /bigquery/v2/projects
func (s *Server) listProjects(w http.ResponseWriter, r *http.Request) {
	projects, err := s.repo.ListProjects(r.Context())
	if err != nil {
		s.logger.Error("failed to list projects")
		apierror.NewInternalError("Failed to list projects").WriteResponse(w)
		return
	}

	projectList := make([]map[string]interface{}, 0, len(projects))
	for _, p := range projects {
		numericID := p.NumericID
		if numericID == 0 {
			numericID = 1
		}
		friendlyName := p.FriendlyName
		if friendlyName == "" {
			friendlyName = p.ID
		}
		projectList = append(projectList, map[string]interface{}{
			"id":        p.ID,
			"numericId": numericID,
			"projectReference": map[string]string{
				"projectId": p.ID,
			},
			"friendlyName": friendlyName,
			"kind":         "bigquery#project",
		})
	}

	resp := map[string]interface{}{
		"kind":       "bigquery#projectList",
		"projects":   projectList,
		"totalItems": len(projects),
	}

	writeJSON(w, http.StatusOK, resp)
}

// getProject handles GET /bigquery/v2/projects/{projectId}
func (s *Server) getProject(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")

	project, err := s.repo.GetProject(r.Context(), projectID)
	if err != nil {
		apierror.NewNotFoundError("Project", fmt.Sprintf("%s", projectID)).WriteResponse(w)
		return
	}

	numericID := project.NumericID
	if numericID == 0 {
		numericID = 1
	}
	friendlyName := project.FriendlyName
	if friendlyName == "" {
		friendlyName = project.ID
	}

	resp := map[string]interface{}{
		"kind":      "bigquery#project",
		"id":        project.ID,
		"numericId": numericID,
		"projectReference": map[string]string{
			"projectId": project.ID,
		},
		"friendlyName": friendlyName,
	}

	writeJSON(w, http.StatusOK, resp)
}
