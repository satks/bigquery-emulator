package permission

import (
	"context"
	"testing"
)

func TestColumnSecurityManager_SetPolicy(t *testing.T) {
	m := NewColumnSecurityManager()
	ctx := context.Background()

	policy := ColumnPolicy{
		ProjectID:  "proj1",
		DatasetID:  "ds1",
		TableID:    "tbl1",
		ColumnName: "ssn",
		Tag: PolicyTag{
			TagID:       "tag-1",
			DisplayName: "PII",
			MaskingType: MaskSHA256,
		},
		FineGrainedReaders: []string{"user:admin@example.com"},
		MaskedReaders:      []string{"user:analyst@example.com"},
	}

	err := m.SetColumnPolicy(ctx, policy)
	if err != nil {
		t.Fatalf("SetColumnPolicy returned error: %v", err)
	}

	// Verify the policy is stored by checking access.
	decision := m.CheckColumnAccess(ctx, "user:admin@example.com", "proj1", "ds1", "tbl1", "ssn")
	if decision != AccessUnmasked {
		t.Errorf("expected AccessUnmasked for fine-grained reader, got %d", decision)
	}
}

func TestColumnSecurityManager_RemovePolicy(t *testing.T) {
	m := NewColumnSecurityManager()
	ctx := context.Background()

	policy := ColumnPolicy{
		ProjectID:  "proj1",
		DatasetID:  "ds1",
		TableID:    "tbl1",
		ColumnName: "ssn",
		Tag: PolicyTag{
			TagID:       "tag-1",
			DisplayName: "PII",
			MaskingType: MaskSHA256,
		},
		FineGrainedReaders: []string{"user:admin@example.com"},
		MaskedReaders:      []string{"user:analyst@example.com"},
	}
	_ = m.SetColumnPolicy(ctx, policy)

	err := m.RemoveColumnPolicy(ctx, "proj1", "ds1", "tbl1", "ssn")
	if err != nil {
		t.Fatalf("RemoveColumnPolicy returned error: %v", err)
	}

	// After removal, access should be unmasked (no policy = no restriction).
	decision := m.CheckColumnAccess(ctx, "user:random@example.com", "proj1", "ds1", "tbl1", "ssn")
	if decision != AccessUnmasked {
		t.Errorf("expected AccessUnmasked after policy removal, got %d", decision)
	}
}

func TestColumnSecurityManager_CheckAccess_NoPolicy(t *testing.T) {
	m := NewColumnSecurityManager()
	ctx := context.Background()

	decision := m.CheckColumnAccess(ctx, "user:anyone@example.com", "proj1", "ds1", "tbl1", "col1")
	if decision != AccessUnmasked {
		t.Errorf("expected AccessUnmasked when no policy exists, got %d", decision)
	}
}

func TestColumnSecurityManager_CheckAccess_FineGrained(t *testing.T) {
	m := NewColumnSecurityManager()
	ctx := context.Background()

	_ = m.SetColumnPolicy(ctx, ColumnPolicy{
		ProjectID:  "proj1",
		DatasetID:  "ds1",
		TableID:    "tbl1",
		ColumnName: "ssn",
		Tag: PolicyTag{
			TagID:       "tag-1",
			DisplayName: "PII",
			MaskingType: MaskSHA256,
		},
		FineGrainedReaders: []string{"user:admin@example.com"},
		MaskedReaders:      []string{"user:analyst@example.com"},
	})

	decision := m.CheckColumnAccess(ctx, "user:admin@example.com", "proj1", "ds1", "tbl1", "ssn")
	if decision != AccessUnmasked {
		t.Errorf("expected AccessUnmasked for fine-grained reader, got %d", decision)
	}
}

func TestColumnSecurityManager_CheckAccess_Masked(t *testing.T) {
	m := NewColumnSecurityManager()
	ctx := context.Background()

	_ = m.SetColumnPolicy(ctx, ColumnPolicy{
		ProjectID:  "proj1",
		DatasetID:  "ds1",
		TableID:    "tbl1",
		ColumnName: "ssn",
		Tag: PolicyTag{
			TagID:       "tag-1",
			DisplayName: "PII",
			MaskingType: MaskSHA256,
		},
		FineGrainedReaders: []string{"user:admin@example.com"},
		MaskedReaders:      []string{"user:analyst@example.com"},
	})

	decision := m.CheckColumnAccess(ctx, "user:analyst@example.com", "proj1", "ds1", "tbl1", "ssn")
	if decision != AccessMasked {
		t.Errorf("expected AccessMasked for masked reader, got %d", decision)
	}
}

func TestColumnSecurityManager_CheckAccess_Denied(t *testing.T) {
	m := NewColumnSecurityManager()
	ctx := context.Background()

	_ = m.SetColumnPolicy(ctx, ColumnPolicy{
		ProjectID:  "proj1",
		DatasetID:  "ds1",
		TableID:    "tbl1",
		ColumnName: "ssn",
		Tag: PolicyTag{
			TagID:       "tag-1",
			DisplayName: "PII",
			MaskingType: MaskSHA256,
		},
		FineGrainedReaders: []string{"user:admin@example.com"},
		MaskedReaders:      []string{"user:analyst@example.com"},
	})

	decision := m.CheckColumnAccess(ctx, "user:stranger@example.com", "proj1", "ds1", "tbl1", "ssn")
	if decision != AccessDenied {
		t.Errorf("expected AccessDenied for unauthorized user, got %d", decision)
	}
}

func TestMaskExpression_SHA256(t *testing.T) {
	m := NewColumnSecurityManager()
	result := m.MaskExpression("email", MaskSHA256, "")
	expected := `sha256(CAST("email" AS VARCHAR))`
	if result != expected {
		t.Errorf("SHA256 mask mismatch\ngot:  %s\nwant: %s", result, expected)
	}
}

func TestMaskExpression_Nullify(t *testing.T) {
	m := NewColumnSecurityManager()
	result := m.MaskExpression("email", MaskNullify, "")
	expected := "NULL"
	if result != expected {
		t.Errorf("Nullify mask mismatch\ngot:  %s\nwant: %s", result, expected)
	}
}

func TestMaskExpression_Default(t *testing.T) {
	m := NewColumnSecurityManager()

	result := m.MaskExpression("email", MaskDefault, "REDACTED")
	expected := "'REDACTED'"
	if result != expected {
		t.Errorf("Default mask mismatch\ngot:  %s\nwant: %s", result, expected)
	}

	// Numeric default.
	result2 := m.MaskExpression("salary", MaskDefault, "0")
	expected2 := "'0'"
	if result2 != expected2 {
		t.Errorf("Default numeric mask mismatch\ngot:  %s\nwant: %s", result2, expected2)
	}
}

func TestMaskExpression_EmailMask(t *testing.T) {
	m := NewColumnSecurityManager()
	result := m.MaskExpression("email", MaskEmailMask, "")
	expected := `regexp_replace("email", '^[^@]+', 'XXXXX')`
	if result != expected {
		t.Errorf("EmailMask mismatch\ngot:  %s\nwant: %s", result, expected)
	}
}

func TestMaskExpression_First4(t *testing.T) {
	m := NewColumnSecurityManager()
	result := m.MaskExpression("phone", MaskFirst4, "")
	expected := `left("phone", 4) || repeat('X', greatest(length("phone") - 4, 0))`
	if result != expected {
		t.Errorf("First4 mask mismatch\ngot:  %s\nwant: %s", result, expected)
	}
}

func TestMaskExpression_Last4(t *testing.T) {
	m := NewColumnSecurityManager()
	result := m.MaskExpression("phone", MaskLast4, "")
	expected := `repeat('X', greatest(length("phone") - 4, 0)) || right("phone", 4)`
	if result != expected {
		t.Errorf("Last4 mask mismatch\ngot:  %s\nwant: %s", result, expected)
	}
}

func TestMaskExpression_DateYear(t *testing.T) {
	m := NewColumnSecurityManager()
	result := m.MaskExpression("birth_date", MaskDateYear, "")
	expected := `date_trunc('year', "birth_date")`
	if result != expected {
		t.Errorf("DateYear mask mismatch\ngot:  %s\nwant: %s", result, expected)
	}
}

func TestApplyColumnMasking_NoPolicy(t *testing.T) {
	m := NewColumnSecurityManager()
	ctx := context.Background()

	expr, allowed := m.ApplyColumnMasking(ctx, "user:anyone@example.com", "proj1", "ds1", "tbl1", "col1")
	if !allowed {
		t.Error("expected access allowed when no policy exists")
	}
	if expr != "col1" {
		t.Errorf("expected column name unchanged, got %q", expr)
	}
}

func TestApplyColumnMasking_Masked(t *testing.T) {
	m := NewColumnSecurityManager()
	ctx := context.Background()

	_ = m.SetColumnPolicy(ctx, ColumnPolicy{
		ProjectID:  "proj1",
		DatasetID:  "ds1",
		TableID:    "tbl1",
		ColumnName: "email",
		Tag: PolicyTag{
			TagID:       "tag-1",
			DisplayName: "PII",
			MaskingType: MaskEmailMask,
		},
		FineGrainedReaders: []string{"user:admin@example.com"},
		MaskedReaders:      []string{"user:analyst@example.com"},
	})

	expr, allowed := m.ApplyColumnMasking(ctx, "user:analyst@example.com", "proj1", "ds1", "tbl1", "email")
	if !allowed {
		t.Error("expected access allowed for masked reader")
	}
	expected := `regexp_replace("email", '^[^@]+', 'XXXXX') AS "email"`
	if expr != expected {
		t.Errorf("masked expression mismatch\ngot:  %s\nwant: %s", expr, expected)
	}
}

func TestApplyColumnMasking_Denied(t *testing.T) {
	m := NewColumnSecurityManager()
	ctx := context.Background()

	_ = m.SetColumnPolicy(ctx, ColumnPolicy{
		ProjectID:  "proj1",
		DatasetID:  "ds1",
		TableID:    "tbl1",
		ColumnName: "ssn",
		Tag: PolicyTag{
			TagID:       "tag-1",
			DisplayName: "PII",
			MaskingType: MaskSHA256,
		},
		FineGrainedReaders: []string{"user:admin@example.com"},
		MaskedReaders:      []string{"user:analyst@example.com"},
	})

	expr, allowed := m.ApplyColumnMasking(ctx, "user:stranger@example.com", "proj1", "ds1", "tbl1", "ssn")
	if allowed {
		t.Error("expected access denied for unauthorized user")
	}
	if expr != "" {
		t.Errorf("expected empty expression for denied access, got %q", expr)
	}
}
