package permission

import (
	"testing"
)

// allPermissions returns every defined Permission constant.
func allPermissions() []Permission {
	return []Permission{
		PermDatasetCreate, PermDatasetGet, PermDatasetList, PermDatasetDelete,
		PermDatasetUpdate, PermDatasetGetIAM, PermDatasetSetIAM,
		PermTableCreate, PermTableGet, PermTableList, PermTableDelete,
		PermTableUpdate, PermTableGetData, PermTableUpdateData,
		PermTableGetIAM, PermTableSetIAM,
		PermJobCreate, PermJobGet, PermJobList, PermJobListAll, PermJobCancel,
		PermRoutineCreate, PermRoutineGet, PermRoutineList, PermRoutineDelete, PermRoutineUpdate,
		PermModelCreate, PermModelGet, PermModelList, PermModelDelete, PermModelGetData,
	}
}

func TestRolePermissions_AdminHasAll(t *testing.T) {
	perms, ok := RolePermissions[RoleAdmin]
	if !ok {
		t.Fatal("Admin role not found in RolePermissions")
	}
	for _, p := range allPermissions() {
		if !perms[p] {
			t.Errorf("Admin role missing permission %s", p)
		}
	}
}

func TestRolePermissions_DataViewerReadOnly(t *testing.T) {
	perms, ok := RolePermissions[RoleDataViewer]
	if !ok {
		t.Fatal("DataViewer role not found in RolePermissions")
	}

	// Should have read-only permissions
	expected := map[Permission]bool{
		PermDatasetGet:  true,
		PermDatasetList: true,
		PermTableGet:    true,
		PermTableList:   true,
		PermTableGetData: true,
		PermRoutineGet:  true,
		PermRoutineList: true,
		PermModelGet:    true,
		PermModelList:   true,
		PermModelGetData: true,
	}

	for p := range expected {
		if !perms[p] {
			t.Errorf("DataViewer should have permission %s", p)
		}
	}

	// Should NOT have write permissions
	writePerms := []Permission{
		PermDatasetCreate, PermDatasetDelete, PermDatasetUpdate,
		PermDatasetGetIAM, PermDatasetSetIAM,
		PermTableCreate, PermTableDelete, PermTableUpdate, PermTableUpdateData,
		PermTableGetIAM, PermTableSetIAM,
		PermRoutineCreate, PermRoutineDelete, PermRoutineUpdate,
		PermModelCreate, PermModelDelete,
	}
	for _, p := range writePerms {
		if perms[p] {
			t.Errorf("DataViewer should NOT have permission %s", p)
		}
	}
}

func TestRolePermissions_DataEditorNoDelete(t *testing.T) {
	perms, ok := RolePermissions[RoleDataEditor]
	if !ok {
		t.Fatal("DataEditor role not found in RolePermissions")
	}

	// DataEditor should NOT have delete permissions
	deletePerms := []Permission{
		PermDatasetDelete, PermTableDelete, PermRoutineDelete, PermModelDelete,
	}
	for _, p := range deletePerms {
		if perms[p] {
			t.Errorf("DataEditor should NOT have permission %s", p)
		}
	}

	// DataEditor should NOT have IAM permissions
	iamPerms := []Permission{
		PermDatasetGetIAM, PermDatasetSetIAM,
		PermTableGetIAM, PermTableSetIAM,
	}
	for _, p := range iamPerms {
		if perms[p] {
			t.Errorf("DataEditor should NOT have permission %s", p)
		}
	}

	// DataEditor should have create/update/updateData
	editPerms := []Permission{
		PermDatasetGet, PermDatasetList,
		PermTableCreate, PermTableGet, PermTableList, PermTableUpdate, PermTableUpdateData,
		PermRoutineCreate, PermRoutineGet, PermRoutineList, PermRoutineUpdate,
		PermModelCreate, PermModelGet, PermModelList, PermModelUpdate,
	}
	for _, p := range editPerms {
		if !perms[p] {
			t.Errorf("DataEditor should have permission %s", p)
		}
	}
}

func TestRolePermissions_DataOwnerHasDelete(t *testing.T) {
	perms, ok := RolePermissions[RoleDataOwner]
	if !ok {
		t.Fatal("DataOwner role not found in RolePermissions")
	}

	// DataOwner should have delete + IAM permissions
	ownerPerms := []Permission{
		PermDatasetCreate, PermDatasetGet, PermDatasetList, PermDatasetDelete,
		PermDatasetUpdate, PermDatasetGetIAM, PermDatasetSetIAM,
		PermTableCreate, PermTableGet, PermTableList, PermTableDelete,
		PermTableUpdate, PermTableGetData, PermTableUpdateData,
		PermTableGetIAM, PermTableSetIAM,
		PermRoutineCreate, PermRoutineGet, PermRoutineList, PermRoutineDelete, PermRoutineUpdate,
		PermModelCreate, PermModelGet, PermModelList, PermModelDelete, PermModelGetData,
	}
	for _, p := range ownerPerms {
		if !perms[p] {
			t.Errorf("DataOwner should have permission %s", p)
		}
	}
}

func TestRolePermissions_JobUserOnlyJobs(t *testing.T) {
	perms, ok := RolePermissions[RoleJobUser]
	if !ok {
		t.Fatal("JobUser role not found in RolePermissions")
	}

	// Should have basic job permissions
	jobPerms := []Permission{PermJobCreate, PermJobGet, PermJobList}
	for _, p := range jobPerms {
		if !perms[p] {
			t.Errorf("JobUser should have permission %s", p)
		}
	}

	// Should NOT have non-job permissions
	nonJobPerms := []Permission{
		PermDatasetCreate, PermDatasetGet, PermTableCreate, PermTableGet,
		PermRoutineGet, PermModelGet,
	}
	for _, p := range nonJobPerms {
		if perms[p] {
			t.Errorf("JobUser should NOT have permission %s", p)
		}
	}

	// Should NOT have advanced job perms
	advancedJobPerms := []Permission{PermJobListAll, PermJobCancel}
	for _, p := range advancedJobPerms {
		if perms[p] {
			t.Errorf("JobUser should NOT have permission %s", p)
		}
	}
}

func TestRolePermissions_UserCombines(t *testing.T) {
	perms, ok := RolePermissions[RoleUser]
	if !ok {
		t.Fatal("User role not found in RolePermissions")
	}

	// User = JobUser + DataViewer
	jobUserPerms := RolePermissions[RoleJobUser]
	dataViewerPerms := RolePermissions[RoleDataViewer]

	// Should have all JobUser permissions
	for p := range jobUserPerms {
		if !perms[p] {
			t.Errorf("User should have JobUser permission %s", p)
		}
	}

	// Should have all DataViewer permissions
	for p := range dataViewerPerms {
		if !perms[p] {
			t.Errorf("User should have DataViewer permission %s", p)
		}
	}

	// Should NOT have permissions beyond JobUser + DataViewer
	combined := make(map[Permission]bool)
	for p := range jobUserPerms {
		combined[p] = true
	}
	for p := range dataViewerPerms {
		combined[p] = true
	}
	for p := range perms {
		if !combined[p] {
			t.Errorf("User should NOT have permission %s (not in JobUser or DataViewer)", p)
		}
	}
}

func TestRolePermissions_MetadataViewerNoGetData(t *testing.T) {
	perms, ok := RolePermissions[RoleMetadataViewer]
	if !ok {
		t.Fatal("MetadataViewer role not found in RolePermissions")
	}

	// Should have metadata-level permissions
	metadataPerms := []Permission{
		PermDatasetGet, PermDatasetList,
		PermTableGet, PermTableList,
		PermRoutineGet, PermRoutineList,
		PermModelGet, PermModelList,
	}
	for _, p := range metadataPerms {
		if !perms[p] {
			t.Errorf("MetadataViewer should have permission %s", p)
		}
	}

	// Should NOT have getData permissions
	dataPerms := []Permission{PermTableGetData, PermModelGetData}
	for _, p := range dataPerms {
		if perms[p] {
			t.Errorf("MetadataViewer should NOT have permission %s", p)
		}
	}

	// Should NOT have any write permissions
	writePerms := []Permission{
		PermDatasetCreate, PermDatasetDelete, PermDatasetUpdate,
		PermTableCreate, PermTableDelete, PermTableUpdate, PermTableUpdateData,
		PermRoutineCreate, PermRoutineDelete, PermRoutineUpdate,
		PermModelCreate, PermModelDelete,
	}
	for _, p := range writePerms {
		if perms[p] {
			t.Errorf("MetadataViewer should NOT have permission %s", p)
		}
	}
}
