package permission

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// RowAccessPolicy defines a row-level security policy.
type RowAccessPolicy struct {
	PolicyID    string
	ProjectID   string
	DatasetID   string
	TableID     string
	PolicyName  string
	FilterSQL   string
	GranteeList []string
}

// tableKey uniquely identifies a table.
type tableKey struct {
	ProjectID string
	DatasetID string
	TableID   string
}

// RLSManager manages row-level security policies.
type RLSManager struct {
	mu       sync.RWMutex
	policies map[tableKey][]RowAccessPolicy
}

// NewRLSManager creates a new RLS manager.
func NewRLSManager() *RLSManager {
	return &RLSManager{
		policies: make(map[tableKey][]RowAccessPolicy),
	}
}

// CreatePolicy creates a row-level security policy.
func (m *RLSManager) CreatePolicy(_ context.Context, policy RowAccessPolicy) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := tableKey{
		ProjectID: policy.ProjectID,
		DatasetID: policy.DatasetID,
		TableID:   policy.TableID,
	}
	m.policies[key] = append(m.policies[key], policy)
	return nil
}

// DropPolicy removes a row-level security policy.
func (m *RLSManager) DropPolicy(_ context.Context, projectID, datasetID, tableID, policyID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := tableKey{
		ProjectID: projectID,
		DatasetID: datasetID,
		TableID:   tableID,
	}
	policies, ok := m.policies[key]
	if !ok {
		return fmt.Errorf("policy %s not found", policyID)
	}
	filtered := make([]RowAccessPolicy, 0, len(policies))
	found := false
	for _, p := range policies {
		if p.PolicyID == policyID {
			found = true
			continue
		}
		filtered = append(filtered, p)
	}
	if !found {
		return fmt.Errorf("policy %s not found", policyID)
	}
	m.policies[key] = filtered
	return nil
}

// ListPolicies returns all policies for a table.
func (m *RLSManager) ListPolicies(_ context.Context, projectID, datasetID, tableID string) []RowAccessPolicy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := tableKey{
		ProjectID: projectID,
		DatasetID: datasetID,
		TableID:   tableID,
	}
	policies, ok := m.policies[key]
	if !ok {
		return []RowAccessPolicy{}
	}
	result := make([]RowAccessPolicy, len(policies))
	copy(result, policies)
	return result
}

// ApplyFilter applies RLS filters to a query for a given user and table.
// If no policies exist, the query is returned unchanged.
// If policies exist but the user is not in any grantee list, WHERE FALSE is applied.
// If the user matches policies, their filters are OR-combined.
// SESSION_USER() in filter SQL is replaced with the user's email.
func (m *RLSManager) ApplyFilter(_ context.Context, query, member, projectID, datasetID, tableID string) string {
	m.mu.RLock()
	key := tableKey{
		ProjectID: projectID,
		DatasetID: datasetID,
		TableID:   tableID,
	}
	policies, ok := m.policies[key]
	if !ok || len(policies) == 0 {
		m.mu.RUnlock()
		return query
	}
	// Copy policies under lock
	pols := make([]RowAccessPolicy, len(policies))
	copy(pols, policies)
	m.mu.RUnlock()

	// Extract email from member string (e.g., "user:alice@example.com" -> "alice@example.com")
	email := member
	if idx := strings.Index(member, ":"); idx >= 0 {
		email = member[idx+1:]
	}

	// Collect matching filters
	var filters []string
	for _, p := range pols {
		for _, g := range p.GranteeList {
			if g == member {
				filterSQL := strings.ReplaceAll(p.FilterSQL, "SESSION_USER()", "'"+email+"'")
				filters = append(filters, "("+filterSQL+")")
				break
			}
		}
	}

	// Build the filter clause
	var filterClause string
	if len(filters) == 0 {
		filterClause = "FALSE"
	} else {
		filterClause = strings.Join(filters, " OR ")
	}

	// Replace table reference with subquery
	tableRef := fmt.Sprintf(`"%s"."%s"`, datasetID, tableID)
	subquery := fmt.Sprintf(`(SELECT * FROM %s WHERE %s) AS "%s"`, tableRef, filterClause, tableID)
	return strings.Replace(query, tableRef, subquery, 1)
}
