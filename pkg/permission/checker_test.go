package permission

import (
	"context"
	"sync"
	"testing"
)

func TestChecker_BypassMode_AlwaysAllows(t *testing.T) {
	c := NewChecker(true)
	// No bindings at all, but bypass mode should allow everything
	err := c.Check(context.Background(), "user:nobody@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
		TableID:   "tbl1",
	}, PermTableDelete)
	if err != nil {
		t.Errorf("bypass mode should allow all checks, got: %v", err)
	}
}

func TestChecker_NoBindings_Denies(t *testing.T) {
	c := NewChecker(false)
	err := c.Check(context.Background(), "user:alice@example.com", Resource{
		ProjectID: "proj1",
	}, PermDatasetCreate)
	if err == nil {
		t.Error("expected denial when no bindings exist")
	}
}

func TestChecker_AdminCanDoAnything(t *testing.T) {
	c := NewChecker(false)
	c.AddBinding(Binding{
		Member:   "user:admin@example.com",
		Role:     RoleAdmin,
		Resource: Resource{ProjectID: "proj1"},
	})

	// Test several different permissions
	perms := []Permission{
		PermDatasetCreate, PermTableDelete, PermJobCreate,
		PermRoutineUpdate, PermModelGetData, PermTableSetIAM,
	}
	for _, p := range perms {
		err := c.Check(context.Background(), "user:admin@example.com", Resource{
			ProjectID: "proj1",
			DatasetID: "ds1",
			TableID:   "tbl1",
		}, p)
		if err != nil {
			t.Errorf("Admin should be allowed %s, got: %v", p, err)
		}
	}
}

func TestChecker_DataViewerCanRead(t *testing.T) {
	c := NewChecker(false)
	c.AddBinding(Binding{
		Member:   "user:viewer@example.com",
		Role:     RoleDataViewer,
		Resource: Resource{ProjectID: "proj1"},
	})

	err := c.Check(context.Background(), "user:viewer@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
		TableID:   "tbl1",
	}, PermTableGetData)
	if err != nil {
		t.Errorf("DataViewer should be able to getData, got: %v", err)
	}
}

func TestChecker_DataViewerCannotCreate(t *testing.T) {
	c := NewChecker(false)
	c.AddBinding(Binding{
		Member:   "user:viewer@example.com",
		Role:     RoleDataViewer,
		Resource: Resource{ProjectID: "proj1"},
	})

	err := c.Check(context.Background(), "user:viewer@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
	}, PermTableCreate)
	if err == nil {
		t.Error("DataViewer should NOT be able to create tables")
	}
}

func TestChecker_DataEditorCanInsert(t *testing.T) {
	c := NewChecker(false)
	c.AddBinding(Binding{
		Member:   "user:editor@example.com",
		Role:     RoleDataEditor,
		Resource: Resource{ProjectID: "proj1"},
	})

	err := c.Check(context.Background(), "user:editor@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
		TableID:   "tbl1",
	}, PermTableUpdateData)
	if err != nil {
		t.Errorf("DataEditor should be able to updateData, got: %v", err)
	}
}

func TestChecker_DataEditorCannotDelete(t *testing.T) {
	c := NewChecker(false)
	c.AddBinding(Binding{
		Member:   "user:editor@example.com",
		Role:     RoleDataEditor,
		Resource: Resource{ProjectID: "proj1"},
	})

	err := c.Check(context.Background(), "user:editor@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
		TableID:   "tbl1",
	}, PermTableDelete)
	if err == nil {
		t.Error("DataEditor should NOT be able to delete tables")
	}
}

func TestChecker_ProjectBindingCascades(t *testing.T) {
	c := NewChecker(false)
	// Binding at project level
	c.AddBinding(Binding{
		Member:   "user:alice@example.com",
		Role:     RoleDataViewer,
		Resource: Resource{ProjectID: "proj1"},
	})

	// Should cascade to dataset level
	err := c.Check(context.Background(), "user:alice@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
	}, PermDatasetGet)
	if err != nil {
		t.Errorf("project binding should cascade to dataset, got: %v", err)
	}

	// Should cascade to table level
	err = c.Check(context.Background(), "user:alice@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
		TableID:   "tbl1",
	}, PermTableGetData)
	if err != nil {
		t.Errorf("project binding should cascade to table, got: %v", err)
	}
}

func TestChecker_DatasetBindingCascadesToTable(t *testing.T) {
	c := NewChecker(false)
	// Binding at dataset level
	c.AddBinding(Binding{
		Member:   "user:bob@example.com",
		Role:     RoleDataViewer,
		Resource: Resource{ProjectID: "proj1", DatasetID: "ds1"},
	})

	// Should cascade to table level within same dataset
	err := c.Check(context.Background(), "user:bob@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
		TableID:   "tbl1",
	}, PermTableGetData)
	if err != nil {
		t.Errorf("dataset binding should cascade to table, got: %v", err)
	}

	// Should NOT grant access to a different dataset
	err = c.Check(context.Background(), "user:bob@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds2",
		TableID:   "tbl1",
	}, PermTableGetData)
	if err == nil {
		t.Error("dataset binding on ds1 should NOT cascade to ds2")
	}
}

func TestChecker_TableBindingDoesNotCascadeUp(t *testing.T) {
	c := NewChecker(false)
	// Binding at table level
	c.AddBinding(Binding{
		Member:   "user:carol@example.com",
		Role:     RoleDataViewer,
		Resource: Resource{ProjectID: "proj1", DatasetID: "ds1", TableID: "tbl1"},
	})

	// Should work for that exact table
	err := c.Check(context.Background(), "user:carol@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
		TableID:   "tbl1",
	}, PermTableGetData)
	if err != nil {
		t.Errorf("table binding should grant access to the same table, got: %v", err)
	}

	// Should NOT cascade up to dataset level
	err = c.Check(context.Background(), "user:carol@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
	}, PermDatasetGet)
	if err == nil {
		t.Error("table binding should NOT cascade up to dataset level")
	}

	// Should NOT cascade to a different table
	err = c.Check(context.Background(), "user:carol@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
		TableID:   "tbl2",
	}, PermTableGetData)
	if err == nil {
		t.Error("table binding on tbl1 should NOT cascade to tbl2")
	}
}

func TestChecker_MultipleRolesUnion(t *testing.T) {
	c := NewChecker(false)
	// Give member both JobUser and DataViewer
	c.AddBinding(Binding{
		Member:   "user:multi@example.com",
		Role:     RoleJobUser,
		Resource: Resource{ProjectID: "proj1"},
	})
	c.AddBinding(Binding{
		Member:   "user:multi@example.com",
		Role:     RoleDataViewer,
		Resource: Resource{ProjectID: "proj1"},
	})

	// Should be able to create jobs (from JobUser)
	err := c.Check(context.Background(), "user:multi@example.com", Resource{
		ProjectID: "proj1",
	}, PermJobCreate)
	if err != nil {
		t.Errorf("member with JobUser should create jobs, got: %v", err)
	}

	// Should be able to read data (from DataViewer)
	err = c.Check(context.Background(), "user:multi@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
		TableID:   "tbl1",
	}, PermTableGetData)
	if err != nil {
		t.Errorf("member with DataViewer should read data, got: %v", err)
	}

	// Should NOT be able to delete (neither role grants it)
	err = c.Check(context.Background(), "user:multi@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
		TableID:   "tbl1",
	}, PermTableDelete)
	if err == nil {
		t.Error("neither JobUser nor DataViewer grants delete")
	}
}

func TestChecker_TestPermissions_ReturnsSubset(t *testing.T) {
	c := NewChecker(false)
	c.AddBinding(Binding{
		Member:   "user:alice@example.com",
		Role:     RoleDataViewer,
		Resource: Resource{ProjectID: "proj1"},
	})

	requested := []Permission{
		PermTableGet, PermTableGetData, PermTableCreate, PermTableDelete,
	}
	granted := c.TestPermissions(context.Background(), "user:alice@example.com", Resource{
		ProjectID: "proj1",
		DatasetID: "ds1",
	}, requested)

	// Should have TableGet and TableGetData, but not TableCreate or TableDelete
	grantedMap := make(map[Permission]bool)
	for _, p := range granted {
		grantedMap[p] = true
	}

	if !grantedMap[PermTableGet] {
		t.Error("TestPermissions should return PermTableGet for DataViewer")
	}
	if !grantedMap[PermTableGetData] {
		t.Error("TestPermissions should return PermTableGetData for DataViewer")
	}
	if grantedMap[PermTableCreate] {
		t.Error("TestPermissions should NOT return PermTableCreate for DataViewer")
	}
	if grantedMap[PermTableDelete] {
		t.Error("TestPermissions should NOT return PermTableDelete for DataViewer")
	}
}

func TestChecker_AddRemoveBinding(t *testing.T) {
	c := NewChecker(false)
	res := Resource{ProjectID: "proj1"}

	c.AddBinding(Binding{
		Member:   "user:alice@example.com",
		Role:     RoleAdmin,
		Resource: res,
	})

	// Verify binding is active
	err := c.Check(context.Background(), "user:alice@example.com", res, PermDatasetCreate)
	if err != nil {
		t.Fatalf("expected access after AddBinding, got: %v", err)
	}

	// Remove the binding
	c.RemoveBinding("user:alice@example.com", RoleAdmin, res)

	// Verify binding is gone
	err = c.Check(context.Background(), "user:alice@example.com", res, PermDatasetCreate)
	if err == nil {
		t.Error("expected denial after RemoveBinding")
	}
}

func TestChecker_GetBindings(t *testing.T) {
	c := NewChecker(false)
	res1 := Resource{ProjectID: "proj1"}
	res2 := Resource{ProjectID: "proj1", DatasetID: "ds1"}

	c.AddBinding(Binding{
		Member:   "user:alice@example.com",
		Role:     RoleAdmin,
		Resource: res1,
	})
	c.AddBinding(Binding{
		Member:   "user:bob@example.com",
		Role:     RoleDataViewer,
		Resource: res1,
	})
	c.AddBinding(Binding{
		Member:   "user:carol@example.com",
		Role:     RoleDataEditor,
		Resource: res2,
	})

	// Get bindings for res1 (project level)
	bindings := c.GetBindings(res1)
	if len(bindings) != 2 {
		t.Fatalf("expected 2 bindings for proj1, got %d", len(bindings))
	}

	// Get bindings for res2 (dataset level)
	bindings = c.GetBindings(res2)
	if len(bindings) != 1 {
		t.Fatalf("expected 1 binding for proj1/ds1, got %d", len(bindings))
	}
	if bindings[0].Member != "user:carol@example.com" {
		t.Errorf("expected carol, got %s", bindings[0].Member)
	}
}

func TestChecker_ConcurrentAccess(t *testing.T) {
	c := NewChecker(false)
	res := Resource{ProjectID: "proj1"}
	c.AddBinding(Binding{
		Member:   "user:alice@example.com",
		Role:     RoleAdmin,
		Resource: res,
	})

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.Check(context.Background(), "user:alice@example.com", res, PermDatasetCreate)
			if err != nil {
				errCh <- err
			}
		}()
	}

	// Concurrent writes
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.AddBinding(Binding{
				Member:   "user:alice@example.com",
				Role:     RoleDataViewer,
				Resource: Resource{ProjectID: "proj1", DatasetID: "ds" + string(rune('0'+i%10))},
			})
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent check failed: %v", err)
	}
}
