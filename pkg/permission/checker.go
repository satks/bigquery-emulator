package permission

import (
	"context"
	"fmt"
	"sync"
)

// Resource identifies a BigQuery resource in the hierarchy.
type Resource struct {
	ProjectID string
	DatasetID string // empty for project-level
	TableID   string // empty for dataset-level
}

// Binding maps a member (email) to a role on a resource.
type Binding struct {
	Member   string   // e.g., "user:alice@example.com"
	Role     Role
	Resource Resource // Which resource this binding applies to
}

// Checker evaluates permissions with hierarchy inheritance.
// When bypassMode is true, all checks pass immediately (for local dev).
type Checker struct {
	mu         sync.RWMutex
	bypassMode bool
	bindings   []Binding
}

// NewChecker creates a permission checker.
// If bypassMode is true, all permission checks pass (local dev mode).
func NewChecker(bypassMode bool) *Checker {
	return &Checker{
		bypassMode: bypassMode,
		bindings:   make([]Binding, 0),
	}
}

// Check verifies that the member has the given permission on the resource.
// Returns nil if allowed, error if denied.
// Permission inheritance: project binding cascades to datasets and tables,
// dataset binding cascades to tables within it.
func (c *Checker) Check(_ context.Context, member string, resource Resource, perm Permission) error {
	c.mu.RLock()
	bypass := c.bypassMode
	c.mu.RUnlock()

	if bypass {
		return nil
	}

	if c.hasPermission(member, resource, perm) {
		return nil
	}

	return fmt.Errorf("permission denied: %s does not have %s on %s",
		member, perm, resourceString(resource))
}

// TestPermissions returns which of the requested permissions the member has.
func (c *Checker) TestPermissions(_ context.Context, member string, resource Resource, perms []Permission) []Permission {
	c.mu.RLock()
	bypass := c.bypassMode
	c.mu.RUnlock()

	if bypass {
		// In bypass mode, all permissions are granted.
		result := make([]Permission, len(perms))
		copy(result, perms)
		return result
	}

	var granted []Permission
	for _, p := range perms {
		if c.hasPermission(member, resource, p) {
			granted = append(granted, p)
		}
	}
	return granted
}

// AddBinding adds a role binding.
func (c *Checker) AddBinding(b Binding) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bindings = append(c.bindings, b)
}

// RemoveBinding removes a role binding.
func (c *Checker) RemoveBinding(member string, role Role, resource Resource) {
	c.mu.Lock()
	defer c.mu.Unlock()

	filtered := make([]Binding, 0, len(c.bindings))
	for _, b := range c.bindings {
		if b.Member == member && b.Role == role && b.Resource == resource {
			continue
		}
		filtered = append(filtered, b)
	}
	c.bindings = filtered
}

// RemoveAllBindings removes all bindings for a resource (any member, any role).
func (c *Checker) RemoveAllBindings(resource Resource) {
	c.mu.Lock()
	defer c.mu.Unlock()

	filtered := make([]Binding, 0, len(c.bindings))
	for _, b := range c.bindings {
		if b.Resource == resource {
			continue
		}
		filtered = append(filtered, b)
	}
	c.bindings = filtered
}

// GetBindings returns all bindings for a resource (exact match only).
func (c *Checker) GetBindings(resource Resource) []Binding {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []Binding
	for _, b := range c.bindings {
		if b.Resource == resource {
			result = append(result, b)
		}
	}
	return result
}

// SetBypassMode enables/disables bypass mode.
func (c *Checker) SetBypassMode(enabled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bypassMode = enabled
}

// IsBypass returns whether bypass mode is active.
func (c *Checker) IsBypass() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.bypassMode
}

// hasPermission checks whether member has perm on resource by collecting
// all matching bindings (exact + parent levels) and unioning their permissions.
func (c *Checker) hasPermission(member string, resource Resource, perm Permission) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, b := range c.bindings {
		if b.Member != member {
			continue
		}
		if !bindingCovers(b.Resource, resource) {
			continue
		}
		rolePerms, ok := RolePermissions[b.Role]
		if !ok {
			continue
		}
		if rolePerms[perm] {
			return true
		}
	}
	return false
}

// bindingCovers returns true if the binding's resource is a parent of (or equal to)
// the target resource in the project > dataset > table hierarchy.
//
// A binding on Resource{ProjectID:"p1"} covers all resources in project p1.
// A binding on Resource{ProjectID:"p1", DatasetID:"d1"} covers all tables in d1.
// A binding on Resource{ProjectID:"p1", DatasetID:"d1", TableID:"t1"} covers only that table.
func bindingCovers(binding, target Resource) bool {
	// Must be same project
	if binding.ProjectID != target.ProjectID {
		return false
	}

	// Project-level binding (dataset empty) covers everything in the project
	if binding.DatasetID == "" {
		return true
	}

	// Dataset-level binding: must match dataset
	if binding.DatasetID != target.DatasetID {
		return false
	}

	// If binding has no table specified, it covers all tables in the dataset
	if binding.TableID == "" {
		return true
	}

	// Table-level binding: must match exact table
	return binding.TableID == target.TableID
}

// resourceString returns a human-readable string for a resource.
func resourceString(r Resource) string {
	s := r.ProjectID
	if r.DatasetID != "" {
		s += "/" + r.DatasetID
	}
	if r.TableID != "" {
		s += "/" + r.TableID
	}
	return s
}
