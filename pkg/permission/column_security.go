package permission

import (
	"context"
	"fmt"
	"sync"
)

// AccessDecision represents the result of a column access check.
type AccessDecision int

const (
	// AccessUnmasked means the user can see the raw data.
	AccessUnmasked AccessDecision = iota
	// AccessMasked means the user sees a masked version.
	AccessMasked
	// AccessDenied means the user cannot access the column at all.
	AccessDenied
)

// MaskingType defines how a column value is masked.
type MaskingType string

const (
	MaskSHA256    MaskingType = "SHA256"
	MaskNullify   MaskingType = "NULLIFY"
	MaskDefault   MaskingType = "DEFAULT_VALUE"
	MaskEmailMask MaskingType = "EMAIL_MASK"
	MaskFirst4    MaskingType = "FIRST_4"
	MaskLast4     MaskingType = "LAST_4"
	MaskDateYear  MaskingType = "DATE_YEAR"
)

// PolicyTag represents a data catalog policy tag for column-level security.
type PolicyTag struct {
	TagID       string
	DisplayName string
	MaskingType MaskingType
}

// ColumnPolicy defines a column-level security policy.
type ColumnPolicy struct {
	ProjectID          string
	DatasetID          string
	TableID            string
	ColumnName         string
	Tag                PolicyTag
	FineGrainedReaders []string // members who see raw data
	MaskedReaders      []string // members who see masked data
}

// columnKey uniquely identifies a column.
type columnKey struct {
	ProjectID string
	DatasetID string
	TableID   string
	Column    string
}

// ColumnSecurityManager manages column-level security policies.
type ColumnSecurityManager struct {
	mu       sync.RWMutex
	policies map[columnKey]ColumnPolicy
}

// NewColumnSecurityManager creates a new column security manager.
func NewColumnSecurityManager() *ColumnSecurityManager {
	return &ColumnSecurityManager{
		policies: make(map[columnKey]ColumnPolicy),
	}
}

// SetColumnPolicy sets a column-level security policy.
func (m *ColumnSecurityManager) SetColumnPolicy(_ context.Context, policy ColumnPolicy) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := columnKey{
		ProjectID: policy.ProjectID,
		DatasetID: policy.DatasetID,
		TableID:   policy.TableID,
		Column:    policy.ColumnName,
	}
	m.policies[key] = policy
	return nil
}

// RemoveColumnPolicy removes a column-level security policy.
func (m *ColumnSecurityManager) RemoveColumnPolicy(_ context.Context, projectID, datasetID, tableID, column string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := columnKey{
		ProjectID: projectID,
		DatasetID: datasetID,
		TableID:   tableID,
		Column:    column,
	}
	delete(m.policies, key)
	return nil
}

// CheckColumnAccess checks a member's access to a column.
func (m *ColumnSecurityManager) CheckColumnAccess(_ context.Context, member, projectID, datasetID, tableID, column string) AccessDecision {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := columnKey{
		ProjectID: projectID,
		DatasetID: datasetID,
		TableID:   tableID,
		Column:    column,
	}
	policy, ok := m.policies[key]
	if !ok {
		return AccessUnmasked
	}

	for _, r := range policy.FineGrainedReaders {
		if r == member {
			return AccessUnmasked
		}
	}
	for _, r := range policy.MaskedReaders {
		if r == member {
			return AccessMasked
		}
	}
	return AccessDenied
}

// MaskExpression returns a SQL expression that masks the column value.
func (m *ColumnSecurityManager) MaskExpression(column string, maskType MaskingType, defaultValue string) string {
	switch maskType {
	case MaskSHA256:
		return fmt.Sprintf(`sha256(CAST("%s" AS VARCHAR))`, column)
	case MaskNullify:
		return "NULL"
	case MaskDefault:
		return fmt.Sprintf("'%s'", defaultValue)
	case MaskEmailMask:
		return fmt.Sprintf(`regexp_replace("%s", '^[^@]+', 'XXXXX')`, column)
	case MaskFirst4:
		return fmt.Sprintf(`left("%s", 4) || repeat('X', greatest(length("%s") - 4, 0))`, column, column)
	case MaskLast4:
		return fmt.Sprintf(`repeat('X', greatest(length("%s") - 4, 0)) || right("%s", 4)`, column, column)
	case MaskDateYear:
		return fmt.Sprintf(`date_trunc('year', "%s")`, column)
	default:
		return fmt.Sprintf(`"%s"`, column)
	}
}

// ApplyColumnMasking returns the SQL expression for a column given the user's access level.
// Returns (expression, allowed). If denied, returns ("", false).
func (m *ColumnSecurityManager) ApplyColumnMasking(ctx context.Context, member, projectID, datasetID, tableID, column string) (string, bool) {
	decision := m.CheckColumnAccess(ctx, member, projectID, datasetID, tableID, column)

	switch decision {
	case AccessUnmasked:
		return column, true
	case AccessMasked:
		m.mu.RLock()
		key := columnKey{
			ProjectID: projectID,
			DatasetID: datasetID,
			TableID:   tableID,
			Column:    column,
		}
		policy := m.policies[key]
		m.mu.RUnlock()
		expr := m.MaskExpression(column, policy.Tag.MaskingType, "")
		return fmt.Sprintf(`%s AS "%s"`, expr, column), true
	case AccessDenied:
		return "", false
	default:
		return "", false
	}
}
