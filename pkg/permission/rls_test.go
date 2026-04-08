package permission

import (
	"context"
	"testing"
)

func TestRLSManager_CreatePolicy(t *testing.T) {
	m := NewRLSManager()
	ctx := context.Background()

	policy := RowAccessPolicy{
		PolicyID:    "policy-1",
		ProjectID:   "proj1",
		DatasetID:   "ds1",
		TableID:     "tbl1",
		PolicyName:  "us_only",
		FilterSQL:   "region = 'US'",
		GranteeList: []string{"user:alice@example.com"},
	}

	err := m.CreatePolicy(ctx, policy)
	if err != nil {
		t.Fatalf("CreatePolicy returned error: %v", err)
	}

	policies := m.ListPolicies(ctx, "proj1", "ds1", "tbl1")
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	if policies[0].PolicyID != "policy-1" {
		t.Errorf("expected PolicyID 'policy-1', got %q", policies[0].PolicyID)
	}
	if policies[0].PolicyName != "us_only" {
		t.Errorf("expected PolicyName 'us_only', got %q", policies[0].PolicyName)
	}
	if policies[0].FilterSQL != "region = 'US'" {
		t.Errorf("expected FilterSQL \"region = 'US'\", got %q", policies[0].FilterSQL)
	}
}

func TestRLSManager_DropPolicy(t *testing.T) {
	m := NewRLSManager()
	ctx := context.Background()

	policy := RowAccessPolicy{
		PolicyID:    "policy-1",
		ProjectID:   "proj1",
		DatasetID:   "ds1",
		TableID:     "tbl1",
		PolicyName:  "us_only",
		FilterSQL:   "region = 'US'",
		GranteeList: []string{"user:alice@example.com"},
	}
	_ = m.CreatePolicy(ctx, policy)

	err := m.DropPolicy(ctx, "proj1", "ds1", "tbl1", "policy-1")
	if err != nil {
		t.Fatalf("DropPolicy returned error: %v", err)
	}

	policies := m.ListPolicies(ctx, "proj1", "ds1", "tbl1")
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies after drop, got %d", len(policies))
	}

	// Dropping a non-existent policy should return an error.
	err = m.DropPolicy(ctx, "proj1", "ds1", "tbl1", "nonexistent")
	if err == nil {
		t.Error("expected error when dropping nonexistent policy")
	}
}

func TestRLSManager_ListPolicies(t *testing.T) {
	m := NewRLSManager()
	ctx := context.Background()

	// Create multiple policies for the same table.
	policies := []RowAccessPolicy{
		{
			PolicyID: "p1", ProjectID: "proj1", DatasetID: "ds1", TableID: "tbl1",
			PolicyName: "us_only", FilterSQL: "region = 'US'",
			GranteeList: []string{"user:alice@example.com"},
		},
		{
			PolicyID: "p2", ProjectID: "proj1", DatasetID: "ds1", TableID: "tbl1",
			PolicyName: "eu_only", FilterSQL: "region = 'EU'",
			GranteeList: []string{"user:bob@example.com"},
		},
		{
			PolicyID: "p3", ProjectID: "proj1", DatasetID: "ds1", TableID: "tbl1",
			PolicyName: "all_regions", FilterSQL: "1=1",
			GranteeList: []string{"user:admin@example.com"},
		},
	}

	for _, p := range policies {
		if err := m.CreatePolicy(ctx, p); err != nil {
			t.Fatalf("CreatePolicy(%s) error: %v", p.PolicyID, err)
		}
	}

	result := m.ListPolicies(ctx, "proj1", "ds1", "tbl1")
	if len(result) != 3 {
		t.Fatalf("expected 3 policies, got %d", len(result))
	}

	// Verify IDs are all present.
	ids := make(map[string]bool)
	for _, p := range result {
		ids[p.PolicyID] = true
	}
	for _, expected := range []string{"p1", "p2", "p3"} {
		if !ids[expected] {
			t.Errorf("missing policy %s in list", expected)
		}
	}
}

func TestRLSManager_ListPolicies_Empty(t *testing.T) {
	m := NewRLSManager()
	ctx := context.Background()

	result := m.ListPolicies(ctx, "proj1", "ds1", "tbl1")
	if result == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 policies, got %d", len(result))
	}
}

func TestRLSManager_ApplyFilter_NoPolicies(t *testing.T) {
	m := NewRLSManager()
	ctx := context.Background()

	query := `SELECT * FROM "ds1"."tbl1" WHERE col > 5`
	result := m.ApplyFilter(ctx, query, "user:alice@example.com", "proj1", "ds1", "tbl1")

	if result != query {
		t.Errorf("expected query unchanged when no policies\ngot:  %s\nwant: %s", result, query)
	}
}

func TestRLSManager_ApplyFilter_UserInGranteeList(t *testing.T) {
	m := NewRLSManager()
	ctx := context.Background()

	policy := RowAccessPolicy{
		PolicyID: "p1", ProjectID: "proj1", DatasetID: "ds1", TableID: "tbl1",
		PolicyName: "us_only", FilterSQL: "region = 'US'",
		GranteeList: []string{"user:alice@example.com"},
	}
	_ = m.CreatePolicy(ctx, policy)

	query := `SELECT * FROM "ds1"."tbl1" WHERE col > 5`
	result := m.ApplyFilter(ctx, query, "user:alice@example.com", "proj1", "ds1", "tbl1")

	// The table reference should be wrapped in a subquery with the RLS filter.
	expected := `SELECT * FROM (SELECT * FROM "ds1"."tbl1" WHERE (region = 'US')) AS "tbl1" WHERE col > 5`
	if result != expected {
		t.Errorf("RLS filter mismatch\ngot:  %s\nwant: %s", result, expected)
	}
}

func TestRLSManager_ApplyFilter_UserNotInGranteeList(t *testing.T) {
	m := NewRLSManager()
	ctx := context.Background()

	policy := RowAccessPolicy{
		PolicyID: "p1", ProjectID: "proj1", DatasetID: "ds1", TableID: "tbl1",
		PolicyName: "us_only", FilterSQL: "region = 'US'",
		GranteeList: []string{"user:alice@example.com"},
	}
	_ = m.CreatePolicy(ctx, policy)

	query := `SELECT * FROM "ds1"."tbl1" WHERE col > 5`
	result := m.ApplyFilter(ctx, query, "user:bob@example.com", "proj1", "ds1", "tbl1")

	// User not in any grantee list -> WHERE FALSE
	expected := `SELECT * FROM (SELECT * FROM "ds1"."tbl1" WHERE FALSE) AS "tbl1" WHERE col > 5`
	if result != expected {
		t.Errorf("RLS filter mismatch\ngot:  %s\nwant: %s", result, expected)
	}
}

func TestRLSManager_ApplyFilter_MultiplePolicies(t *testing.T) {
	m := NewRLSManager()
	ctx := context.Background()

	// Two policies the user matches: should be OR-combined.
	_ = m.CreatePolicy(ctx, RowAccessPolicy{
		PolicyID: "p1", ProjectID: "proj1", DatasetID: "ds1", TableID: "tbl1",
		PolicyName: "us_only", FilterSQL: "region = 'US'",
		GranteeList: []string{"user:alice@example.com"},
	})
	_ = m.CreatePolicy(ctx, RowAccessPolicy{
		PolicyID: "p2", ProjectID: "proj1", DatasetID: "ds1", TableID: "tbl1",
		PolicyName: "eu_only", FilterSQL: "region = 'EU'",
		GranteeList: []string{"user:alice@example.com", "user:bob@example.com"},
	})
	// Third policy alice does NOT match.
	_ = m.CreatePolicy(ctx, RowAccessPolicy{
		PolicyID: "p3", ProjectID: "proj1", DatasetID: "ds1", TableID: "tbl1",
		PolicyName: "asia_only", FilterSQL: "region = 'ASIA'",
		GranteeList: []string{"user:charlie@example.com"},
	})

	query := `SELECT * FROM "ds1"."tbl1" WHERE col > 5`
	result := m.ApplyFilter(ctx, query, "user:alice@example.com", "proj1", "ds1", "tbl1")

	// Alice matches p1 and p2 -> OR-combined.
	expected := `SELECT * FROM (SELECT * FROM "ds1"."tbl1" WHERE (region = 'US') OR (region = 'EU')) AS "tbl1" WHERE col > 5`
	if result != expected {
		t.Errorf("RLS filter mismatch\ngot:  %s\nwant: %s", result, expected)
	}
}

func TestRLSManager_ApplyFilter_SessionUser(t *testing.T) {
	m := NewRLSManager()
	ctx := context.Background()

	policy := RowAccessPolicy{
		PolicyID: "p1", ProjectID: "proj1", DatasetID: "ds1", TableID: "tbl1",
		PolicyName: "own_data", FilterSQL: "user_email = SESSION_USER()",
		GranteeList: []string{"user:alice@example.com"},
	}
	_ = m.CreatePolicy(ctx, policy)

	query := `SELECT * FROM "ds1"."tbl1"`
	result := m.ApplyFilter(ctx, query, "user:alice@example.com", "proj1", "ds1", "tbl1")

	// SESSION_USER() should be replaced with the user's email.
	expected := `SELECT * FROM (SELECT * FROM "ds1"."tbl1" WHERE (user_email = 'alice@example.com')) AS "tbl1"`
	if result != expected {
		t.Errorf("SESSION_USER() replacement mismatch\ngot:  %s\nwant: %s", result, expected)
	}
}
