package permission

import (
	"context"
	"fmt"
)

// ACLRole represents a legacy BigQuery dataset access role.
type ACLRole string

const (
	ACLOwner  ACLRole = "OWNER"
	ACLWriter ACLRole = "WRITER"
	ACLReader ACLRole = "READER"
)

// aclRoleLevel assigns a numeric level for hierarchy comparison.
// Higher level satisfies lower level checks.
var aclRoleLevel = map[ACLRole]int{
	ACLOwner:  3,
	ACLWriter: 2,
	ACLReader: 1,
}

// ACLEntry represents a dataset-level access control entry.
type ACLEntry struct {
	Role         ACLRole
	UserByEmail  string // e.g., "alice@example.com"
	GroupByEmail string // e.g., "analysts@example.com"
	Domain       string // e.g., "example.com"
	SpecialGroup string // "projectOwners", "projectWriters", "projectReaders", "allAuthenticatedUsers"
	IAMMember    string // e.g., "user:alice@example.com"
}

// ACLRoleToIAMRole maps legacy ACL roles to IAM roles.
// OWNER -> RoleDataOwner, WRITER -> RoleDataEditor, READER -> RoleDataViewer.
func ACLRoleToIAMRole(aclRole ACLRole) Role {
	switch aclRole {
	case ACLOwner:
		return RoleDataOwner
	case ACLWriter:
		return RoleDataEditor
	case ACLReader:
		return RoleDataViewer
	default:
		return RoleDataViewer
	}
}

// IAMRoleToACLRole maps IAM roles back to legacy ACL roles (best effort).
// Returns the ACL role and true if a mapping exists, or ("", false) otherwise.
func IAMRoleToACLRole(iamRole Role) (ACLRole, bool) {
	switch iamRole {
	case RoleDataOwner, RoleAdmin:
		return ACLOwner, true
	case RoleDataEditor:
		return ACLWriter, true
	case RoleDataViewer:
		return ACLReader, true
	default:
		return "", false
	}
}

// ACLManager manages dataset-level access control lists.
type ACLManager struct {
	checker *Checker
}

// NewACLManager creates a new ACL manager backed by the permission checker.
func NewACLManager(checker *Checker) *ACLManager {
	return &ACLManager{checker: checker}
}

// SetACL replaces the ACL for a dataset.
// Removes all existing bindings for the dataset, then adds new ones from entries.
func (m *ACLManager) SetACL(ctx context.Context, projectID, datasetID string, entries []ACLEntry) error {
	resource := Resource{ProjectID: projectID, DatasetID: datasetID}

	// Remove all existing bindings for this dataset
	existing := m.checker.GetBindings(resource)
	for _, b := range existing {
		m.checker.RemoveBinding(b.Member, b.Role, resource)
	}

	// Add new bindings from entries
	for _, entry := range entries {
		member := entryToMember(entry)
		if member == "" {
			continue
		}
		iamRole := ACLRoleToIAMRole(entry.Role)
		m.checker.AddBinding(Binding{
			Member:   member,
			Role:     iamRole,
			Resource: resource,
		})
	}
	return nil
}

// GetACL returns the current ACL entries for a dataset.
func (m *ACLManager) GetACL(ctx context.Context, projectID, datasetID string) []ACLEntry {
	resource := Resource{ProjectID: projectID, DatasetID: datasetID}
	bindings := m.checker.GetBindings(resource)

	var entries []ACLEntry
	for _, b := range bindings {
		aclRole, ok := IAMRoleToACLRole(b.Role)
		if !ok {
			continue
		}
		entry := memberToEntry(b.Member, aclRole)
		entries = append(entries, entry)
	}
	return entries
}

// CheckAccess checks if a member has the required ACL role on a dataset.
// OWNER satisfies WRITER and READER checks.
// WRITER satisfies READER checks.
func (m *ACLManager) CheckAccess(ctx context.Context, member string, projectID, datasetID string, requiredRole ACLRole) error {
	resource := Resource{ProjectID: projectID, DatasetID: datasetID}
	bindings := m.checker.GetBindings(resource)

	requiredLevel := aclRoleLevel[requiredRole]

	for _, b := range bindings {
		// Check if this binding applies to the member
		if !bindingMatchesMember(b, member) {
			continue
		}

		// Convert IAM role to ACL role and check hierarchy
		aclRole, ok := IAMRoleToACLRole(b.Role)
		if !ok {
			continue
		}

		memberLevel := aclRoleLevel[aclRole]
		if memberLevel >= requiredLevel {
			return nil
		}
	}

	return fmt.Errorf("access denied: %s does not have %s on %s.%s", member, requiredRole, projectID, datasetID)
}

// bindingMatchesMember checks if a binding applies to the given member.
// Handles direct member match and special groups like allAuthenticatedUsers.
func bindingMatchesMember(b Binding, member string) bool {
	if b.Member == member {
		return true
	}
	// allAuthenticatedUsers matches any member
	if b.Member == "special:allAuthenticatedUsers" {
		return true
	}
	return false
}

// entryToMember converts an ACLEntry to a member string for the Checker.
// UserByEmail -> "user:email"
// GroupByEmail -> "group:email"
// Domain -> "domain:domain"
// SpecialGroup -> "special:group"
// IAMMember -> as-is
func entryToMember(entry ACLEntry) string {
	switch {
	case entry.UserByEmail != "":
		return "user:" + entry.UserByEmail
	case entry.GroupByEmail != "":
		return "group:" + entry.GroupByEmail
	case entry.Domain != "":
		return "domain:" + entry.Domain
	case entry.SpecialGroup != "":
		return "special:" + entry.SpecialGroup
	case entry.IAMMember != "":
		return entry.IAMMember
	default:
		return ""
	}
}

// memberToEntry converts a member string back to an ACLEntry.
func memberToEntry(member string, role ACLRole) ACLEntry {
	entry := ACLEntry{Role: role}

	if len(member) < 3 {
		entry.IAMMember = member
		return entry
	}

	// Parse "prefix:value" format
	for i := 0; i < len(member); i++ {
		if member[i] == ':' {
			prefix := member[:i]
			value := member[i+1:]
			switch prefix {
			case "user":
				entry.UserByEmail = value
			case "group":
				entry.GroupByEmail = value
			case "domain":
				entry.Domain = value
			case "special":
				entry.SpecialGroup = value
			default:
				entry.IAMMember = member
			}
			return entry
		}
	}

	entry.IAMMember = member
	return entry
}
