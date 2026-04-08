package permission

import (
	"context"
	"testing"
)

func TestACLRoleToIAMRole_Owner(t *testing.T) {
	got := ACLRoleToIAMRole(ACLOwner)
	if got != RoleDataOwner {
		t.Errorf("ACLRoleToIAMRole(OWNER) = %q, want %q", got, RoleDataOwner)
	}
}

func TestACLRoleToIAMRole_Writer(t *testing.T) {
	got := ACLRoleToIAMRole(ACLWriter)
	if got != RoleDataEditor {
		t.Errorf("ACLRoleToIAMRole(WRITER) = %q, want %q", got, RoleDataEditor)
	}
}

func TestACLRoleToIAMRole_Reader(t *testing.T) {
	got := ACLRoleToIAMRole(ACLReader)
	if got != RoleDataViewer {
		t.Errorf("ACLRoleToIAMRole(READER) = %q, want %q", got, RoleDataViewer)
	}
}

func TestACLManager_SetACL_And_GetACL(t *testing.T) {
	checker := NewChecker(false)
	mgr := NewACLManager(checker)
	ctx := context.Background()

	entries := []ACLEntry{
		{Role: ACLOwner, UserByEmail: "alice@example.com"},
		{Role: ACLReader, GroupByEmail: "analysts@example.com"},
		{Role: ACLWriter, Domain: "example.com"},
	}

	err := mgr.SetACL(ctx, "project1", "dataset1", entries)
	if err != nil {
		t.Fatalf("SetACL failed: %v", err)
	}

	got := mgr.GetACL(ctx, "project1", "dataset1")
	if len(got) != 3 {
		t.Fatalf("GetACL returned %d entries, want 3", len(got))
	}

	// Verify entries are returned correctly
	foundAlice := false
	foundAnalysts := false
	foundDomain := false
	for _, e := range got {
		switch {
		case e.UserByEmail == "alice@example.com" && e.Role == ACLOwner:
			foundAlice = true
		case e.GroupByEmail == "analysts@example.com" && e.Role == ACLReader:
			foundAnalysts = true
		case e.Domain == "example.com" && e.Role == ACLWriter:
			foundDomain = true
		}
	}
	if !foundAlice {
		t.Error("missing ACL entry for alice@example.com OWNER")
	}
	if !foundAnalysts {
		t.Error("missing ACL entry for analysts@example.com READER")
	}
	if !foundDomain {
		t.Error("missing ACL entry for example.com WRITER")
	}
}

func TestACLManager_CheckAccess_OwnerSatisfiesAll(t *testing.T) {
	checker := NewChecker(false)
	mgr := NewACLManager(checker)
	ctx := context.Background()

	entries := []ACLEntry{
		{Role: ACLOwner, UserByEmail: "alice@example.com"},
	}
	if err := mgr.SetACL(ctx, "project1", "dataset1", entries); err != nil {
		t.Fatalf("SetACL failed: %v", err)
	}

	// OWNER should satisfy OWNER check
	if err := mgr.CheckAccess(ctx, "user:alice@example.com", "project1", "dataset1", ACLOwner); err != nil {
		t.Errorf("OWNER should satisfy OWNER check: %v", err)
	}

	// OWNER should satisfy WRITER check
	if err := mgr.CheckAccess(ctx, "user:alice@example.com", "project1", "dataset1", ACLWriter); err != nil {
		t.Errorf("OWNER should satisfy WRITER check: %v", err)
	}

	// OWNER should satisfy READER check
	if err := mgr.CheckAccess(ctx, "user:alice@example.com", "project1", "dataset1", ACLReader); err != nil {
		t.Errorf("OWNER should satisfy READER check: %v", err)
	}
}

func TestACLManager_CheckAccess_WriterSatisfiesReader(t *testing.T) {
	checker := NewChecker(false)
	mgr := NewACLManager(checker)
	ctx := context.Background()

	entries := []ACLEntry{
		{Role: ACLWriter, UserByEmail: "bob@example.com"},
	}
	if err := mgr.SetACL(ctx, "project1", "dataset1", entries); err != nil {
		t.Fatalf("SetACL failed: %v", err)
	}

	// WRITER should satisfy WRITER check
	if err := mgr.CheckAccess(ctx, "user:bob@example.com", "project1", "dataset1", ACLWriter); err != nil {
		t.Errorf("WRITER should satisfy WRITER check: %v", err)
	}

	// WRITER should satisfy READER check
	if err := mgr.CheckAccess(ctx, "user:bob@example.com", "project1", "dataset1", ACLReader); err != nil {
		t.Errorf("WRITER should satisfy READER check: %v", err)
	}
}

func TestACLManager_CheckAccess_ReaderCannotWrite(t *testing.T) {
	checker := NewChecker(false)
	mgr := NewACLManager(checker)
	ctx := context.Background()

	entries := []ACLEntry{
		{Role: ACLReader, UserByEmail: "carol@example.com"},
	}
	if err := mgr.SetACL(ctx, "project1", "dataset1", entries); err != nil {
		t.Fatalf("SetACL failed: %v", err)
	}

	// READER should satisfy READER check
	if err := mgr.CheckAccess(ctx, "user:carol@example.com", "project1", "dataset1", ACLReader); err != nil {
		t.Errorf("READER should satisfy READER check: %v", err)
	}

	// READER should NOT satisfy WRITER check
	if err := mgr.CheckAccess(ctx, "user:carol@example.com", "project1", "dataset1", ACLWriter); err == nil {
		t.Error("READER should NOT satisfy WRITER check")
	}

	// READER should NOT satisfy OWNER check
	if err := mgr.CheckAccess(ctx, "user:carol@example.com", "project1", "dataset1", ACLOwner); err == nil {
		t.Error("READER should NOT satisfy OWNER check")
	}
}

func TestACLManager_CheckAccess_NoACL_Denied(t *testing.T) {
	checker := NewChecker(false)
	mgr := NewACLManager(checker)
	ctx := context.Background()

	// No ACL set for this dataset
	err := mgr.CheckAccess(ctx, "user:unknown@example.com", "project1", "dataset1", ACLReader)
	if err == nil {
		t.Error("expected access denied when no ACL is set")
	}
}

func TestACLManager_CheckAccess_SpecialGroup_AllAuth(t *testing.T) {
	checker := NewChecker(false)
	mgr := NewACLManager(checker)
	ctx := context.Background()

	entries := []ACLEntry{
		{Role: ACLReader, SpecialGroup: "allAuthenticatedUsers"},
	}
	if err := mgr.SetACL(ctx, "project1", "dataset1", entries); err != nil {
		t.Fatalf("SetACL failed: %v", err)
	}

	// Any authenticated user should have read access
	if err := mgr.CheckAccess(ctx, "user:anyone@example.com", "project1", "dataset1", ACLReader); err != nil {
		t.Errorf("allAuthenticatedUsers should grant READER access to any user: %v", err)
	}

	// But allAuthenticatedUsers with READER should not grant WRITER
	if err := mgr.CheckAccess(ctx, "user:anyone@example.com", "project1", "dataset1", ACLWriter); err == nil {
		t.Error("allAuthenticatedUsers with READER should NOT grant WRITER access")
	}
}

func TestACLManager_CheckAccess_EntityTypes(t *testing.T) {
	checker := NewChecker(false)
	mgr := NewACLManager(checker)
	ctx := context.Background()

	entries := []ACLEntry{
		{Role: ACLReader, UserByEmail: "alice@example.com"},
		{Role: ACLWriter, GroupByEmail: "devs@example.com"},
		{Role: ACLOwner, Domain: "example.com"},
	}
	if err := mgr.SetACL(ctx, "project1", "dataset1", entries); err != nil {
		t.Fatalf("SetACL failed: %v", err)
	}

	// userByEmail
	if err := mgr.CheckAccess(ctx, "user:alice@example.com", "project1", "dataset1", ACLReader); err != nil {
		t.Errorf("userByEmail should work: %v", err)
	}

	// groupByEmail
	if err := mgr.CheckAccess(ctx, "group:devs@example.com", "project1", "dataset1", ACLWriter); err != nil {
		t.Errorf("groupByEmail should work: %v", err)
	}

	// domain
	if err := mgr.CheckAccess(ctx, "domain:example.com", "project1", "dataset1", ACLOwner); err != nil {
		t.Errorf("domain should work: %v", err)
	}
}

func TestACLManager_SetACL_Replaces(t *testing.T) {
	checker := NewChecker(false)
	mgr := NewACLManager(checker)
	ctx := context.Background()

	// Set initial ACL
	entries1 := []ACLEntry{
		{Role: ACLOwner, UserByEmail: "alice@example.com"},
		{Role: ACLReader, UserByEmail: "bob@example.com"},
	}
	if err := mgr.SetACL(ctx, "project1", "dataset1", entries1); err != nil {
		t.Fatalf("SetACL (first) failed: %v", err)
	}

	// Verify alice has OWNER
	if err := mgr.CheckAccess(ctx, "user:alice@example.com", "project1", "dataset1", ACLOwner); err != nil {
		t.Fatalf("alice should have OWNER after first SetACL: %v", err)
	}

	// Replace with new ACL (alice removed, carol added)
	entries2 := []ACLEntry{
		{Role: ACLWriter, UserByEmail: "carol@example.com"},
	}
	if err := mgr.SetACL(ctx, "project1", "dataset1", entries2); err != nil {
		t.Fatalf("SetACL (second) failed: %v", err)
	}

	// alice should no longer have access
	if err := mgr.CheckAccess(ctx, "user:alice@example.com", "project1", "dataset1", ACLReader); err == nil {
		t.Error("alice should have been removed by second SetACL")
	}

	// bob should no longer have access
	if err := mgr.CheckAccess(ctx, "user:bob@example.com", "project1", "dataset1", ACLReader); err == nil {
		t.Error("bob should have been removed by second SetACL")
	}

	// carol should have WRITER
	if err := mgr.CheckAccess(ctx, "user:carol@example.com", "project1", "dataset1", ACLWriter); err != nil {
		t.Errorf("carol should have WRITER after second SetACL: %v", err)
	}

	// Verify GetACL returns only the new entries
	got := mgr.GetACL(ctx, "project1", "dataset1")
	if len(got) != 1 {
		t.Fatalf("GetACL returned %d entries, want 1", len(got))
	}
}
