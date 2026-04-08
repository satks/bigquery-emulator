package permission

// Permission represents a BigQuery IAM permission.
type Permission string

const (
	// Dataset permissions
	PermDatasetCreate Permission = "bigquery.datasets.create"
	PermDatasetGet    Permission = "bigquery.datasets.get"
	PermDatasetList   Permission = "bigquery.datasets.list"
	PermDatasetDelete Permission = "bigquery.datasets.delete"
	PermDatasetUpdate Permission = "bigquery.datasets.update"
	PermDatasetGetIAM Permission = "bigquery.datasets.getIamPolicy"
	PermDatasetSetIAM Permission = "bigquery.datasets.setIamPolicy"

	// Table permissions
	PermTableCreate     Permission = "bigquery.tables.create"
	PermTableGet        Permission = "bigquery.tables.get"
	PermTableList       Permission = "bigquery.tables.list"
	PermTableDelete     Permission = "bigquery.tables.delete"
	PermTableUpdate     Permission = "bigquery.tables.update"
	PermTableGetData    Permission = "bigquery.tables.getData"
	PermTableUpdateData Permission = "bigquery.tables.updateData"
	PermTableGetIAM     Permission = "bigquery.tables.getIamPolicy"
	PermTableSetIAM     Permission = "bigquery.tables.setIamPolicy"

	// Job permissions
	PermJobCreate  Permission = "bigquery.jobs.create"
	PermJobGet     Permission = "bigquery.jobs.get"
	PermJobList    Permission = "bigquery.jobs.list"
	PermJobListAll Permission = "bigquery.jobs.listAll"
	PermJobCancel  Permission = "bigquery.jobs.cancel"

	// Routine permissions
	PermRoutineCreate Permission = "bigquery.routines.create"
	PermRoutineGet    Permission = "bigquery.routines.get"
	PermRoutineList   Permission = "bigquery.routines.list"
	PermRoutineDelete Permission = "bigquery.routines.delete"
	PermRoutineUpdate Permission = "bigquery.routines.update"

	// Model permissions
	PermModelCreate  Permission = "bigquery.models.create"
	PermModelGet     Permission = "bigquery.models.get"
	PermModelList    Permission = "bigquery.models.list"
	PermModelDelete  Permission = "bigquery.models.delete"
	PermModelGetData Permission = "bigquery.models.getData"
	PermModelUpdate  Permission = "bigquery.models.update"
)

// Role represents a predefined BigQuery IAM role.
type Role string

const (
	RoleAdmin          Role = "roles/bigquery.admin"
	RoleDataOwner      Role = "roles/bigquery.dataOwner"
	RoleDataEditor     Role = "roles/bigquery.dataEditor"
	RoleDataViewer     Role = "roles/bigquery.dataViewer"
	RoleJobUser        Role = "roles/bigquery.jobUser"
	RoleUser           Role = "roles/bigquery.user"
	RoleMetadataViewer Role = "roles/bigquery.metadataViewer"
)

// RolePermissions maps each predefined role to the permissions it grants.
var RolePermissions map[Role]map[Permission]bool

func init() {
	RolePermissions = make(map[Role]map[Permission]bool)

	// Admin: ALL permissions
	RolePermissions[RoleAdmin] = permSet(
		PermDatasetCreate, PermDatasetGet, PermDatasetList, PermDatasetDelete,
		PermDatasetUpdate, PermDatasetGetIAM, PermDatasetSetIAM,
		PermTableCreate, PermTableGet, PermTableList, PermTableDelete,
		PermTableUpdate, PermTableGetData, PermTableUpdateData,
		PermTableGetIAM, PermTableSetIAM,
		PermJobCreate, PermJobGet, PermJobList, PermJobListAll, PermJobCancel,
		PermRoutineCreate, PermRoutineGet, PermRoutineList, PermRoutineDelete, PermRoutineUpdate,
		PermModelCreate, PermModelGet, PermModelList, PermModelDelete, PermModelGetData, PermModelUpdate,
	)

	// DataOwner: All dataset + table + routine + model permissions (including delete, getIAM, setIAM)
	RolePermissions[RoleDataOwner] = permSet(
		PermDatasetCreate, PermDatasetGet, PermDatasetList, PermDatasetDelete,
		PermDatasetUpdate, PermDatasetGetIAM, PermDatasetSetIAM,
		PermTableCreate, PermTableGet, PermTableList, PermTableDelete,
		PermTableUpdate, PermTableGetData, PermTableUpdateData,
		PermTableGetIAM, PermTableSetIAM,
		PermRoutineCreate, PermRoutineGet, PermRoutineList, PermRoutineDelete, PermRoutineUpdate,
		PermModelCreate, PermModelGet, PermModelList, PermModelDelete, PermModelGetData, PermModelUpdate,
	)

	// DataEditor: dataset.get/list, tables.create/get/list/update/updateData,
	// routines.create/get/list/update, models.create/get/list/update
	// (NO delete, NO getIAM/setIAM)
	RolePermissions[RoleDataEditor] = permSet(
		PermDatasetGet, PermDatasetList,
		PermTableCreate, PermTableGet, PermTableList, PermTableUpdate, PermTableUpdateData,
		PermRoutineCreate, PermRoutineGet, PermRoutineList, PermRoutineUpdate,
		PermModelCreate, PermModelGet, PermModelList, PermModelUpdate,
	)

	// DataViewer: dataset.get/list, tables.get/list/getData,
	// routines.get/list, models.get/list/getData (read-only)
	RolePermissions[RoleDataViewer] = permSet(
		PermDatasetGet, PermDatasetList,
		PermTableGet, PermTableList, PermTableGetData,
		PermRoutineGet, PermRoutineList,
		PermModelGet, PermModelList, PermModelGetData,
	)

	// JobUser: jobs.create/get/list (project-level only)
	RolePermissions[RoleJobUser] = permSet(
		PermJobCreate, PermJobGet, PermJobList,
	)

	// User: JobUser + DataViewer combined
	userPerms := make(map[Permission]bool)
	for p := range RolePermissions[RoleJobUser] {
		userPerms[p] = true
	}
	for p := range RolePermissions[RoleDataViewer] {
		userPerms[p] = true
	}
	RolePermissions[RoleUser] = userPerms

	// MetadataViewer: dataset.get/list, tables.get/list (NO getData),
	// routines.get/list, models.get/list (NO getData)
	RolePermissions[RoleMetadataViewer] = permSet(
		PermDatasetGet, PermDatasetList,
		PermTableGet, PermTableList,
		PermRoutineGet, PermRoutineList,
		PermModelGet, PermModelList,
	)
}

// permSet is a helper that creates a permission set from a list of permissions.
func permSet(perms ...Permission) map[Permission]bool {
	m := make(map[Permission]bool, len(perms))
	for _, p := range perms {
		m[p] = true
	}
	return m
}
