package types

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestBQFieldToArrowField_Int64(t *testing.T) {
	field := FieldSchema{Name: "count", Type: BQInt64, Mode: ModeNullable}
	af, err := BQFieldToArrowField(field)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if af.Name != "count" {
		t.Errorf("expected name 'count', got %q", af.Name)
	}
	if af.Type != arrow.PrimitiveTypes.Int64 {
		t.Errorf("expected Int64 type, got %v", af.Type)
	}
	if !af.Nullable {
		t.Error("expected nullable field")
	}
}

func TestBQFieldToArrowField_String(t *testing.T) {
	field := FieldSchema{Name: "name", Type: BQString, Mode: ModeRequired}
	af, err := BQFieldToArrowField(field)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if af.Name != "name" {
		t.Errorf("expected name 'name', got %q", af.Name)
	}
	if af.Type != arrow.BinaryTypes.String {
		t.Errorf("expected String type, got %v", af.Type)
	}
	if af.Nullable {
		t.Error("expected non-nullable field for REQUIRED mode")
	}
}

func TestBQFieldToArrowField_Bool(t *testing.T) {
	field := FieldSchema{Name: "active", Type: BQBool}
	af, err := BQFieldToArrowField(field)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if af.Name != "active" {
		t.Errorf("expected name 'active', got %q", af.Name)
	}
	if af.Type != arrow.FixedWidthTypes.Boolean {
		t.Errorf("expected Boolean type, got %v", af.Type)
	}
}

func TestBQFieldToArrowField_Timestamp(t *testing.T) {
	field := FieldSchema{Name: "created_at", Type: BQTimestamp}
	af, err := BQFieldToArrowField(field)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if af.Name != "created_at" {
		t.Errorf("expected name 'created_at', got %q", af.Name)
	}
	tsType, ok := af.Type.(*arrow.TimestampType)
	if !ok {
		t.Fatalf("expected TimestampType, got %T", af.Type)
	}
	if tsType.Unit != arrow.Microsecond {
		t.Errorf("expected Microsecond unit, got %v", tsType.Unit)
	}
}

func TestBQFieldToArrowField_Repeated(t *testing.T) {
	field := FieldSchema{Name: "tags", Type: BQString, Mode: ModeRepeated}
	af, err := BQFieldToArrowField(field)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if af.Name != "tags" {
		t.Errorf("expected name 'tags', got %q", af.Name)
	}
	listType, ok := af.Type.(*arrow.ListType)
	if !ok {
		t.Fatalf("expected ListType for REPEATED, got %T", af.Type)
	}
	if listType.Elem() != arrow.BinaryTypes.String {
		t.Errorf("expected list element type String, got %v", listType.Elem())
	}
}

func TestBQFieldToArrowField_Struct(t *testing.T) {
	field := FieldSchema{
		Name: "address",
		Type: BQStruct,
		Fields: []FieldSchema{
			{Name: "city", Type: BQString},
			{Name: "zip", Type: BQInt64},
		},
	}
	af, err := BQFieldToArrowField(field)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if af.Name != "address" {
		t.Errorf("expected name 'address', got %q", af.Name)
	}
	structType, ok := af.Type.(*arrow.StructType)
	if !ok {
		t.Fatalf("expected StructType, got %T", af.Type)
	}
	if structType.NumFields() != 2 {
		t.Fatalf("expected 2 fields, got %d", structType.NumFields())
	}
	if structType.Field(0).Name != "city" {
		t.Errorf("expected first field 'city', got %q", structType.Field(0).Name)
	}
	if structType.Field(0).Type != arrow.BinaryTypes.String {
		t.Errorf("expected city type String, got %v", structType.Field(0).Type)
	}
	if structType.Field(1).Name != "zip" {
		t.Errorf("expected second field 'zip', got %q", structType.Field(1).Name)
	}
	if structType.Field(1).Type != arrow.PrimitiveTypes.Int64 {
		t.Errorf("expected zip type Int64, got %v", structType.Field(1).Type)
	}
}

func TestBQSchemaToArrowSchema(t *testing.T) {
	schema := TableSchema{
		Fields: []FieldSchema{
			{Name: "id", Type: BQInt64, Mode: ModeRequired},
			{Name: "name", Type: BQString},
			{Name: "score", Type: BQFloat64},
		},
	}
	arrowSchema, err := BQSchemaToArrowSchema(schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if arrowSchema.NumFields() != 3 {
		t.Fatalf("expected 3 fields, got %d", arrowSchema.NumFields())
	}
	if arrowSchema.Field(0).Name != "id" {
		t.Errorf("expected field 0 name 'id', got %q", arrowSchema.Field(0).Name)
	}
	if arrowSchema.Field(0).Type != arrow.PrimitiveTypes.Int64 {
		t.Errorf("expected field 0 type Int64, got %v", arrowSchema.Field(0).Type)
	}
	if arrowSchema.Field(0).Nullable {
		t.Error("expected field 0 non-nullable (REQUIRED)")
	}
	if arrowSchema.Field(1).Name != "name" {
		t.Errorf("expected field 1 name 'name', got %q", arrowSchema.Field(1).Name)
	}
	if arrowSchema.Field(2).Name != "score" {
		t.Errorf("expected field 2 name 'score', got %q", arrowSchema.Field(2).Name)
	}
}

func TestArrowSchemaToTableSchema_Roundtrip(t *testing.T) {
	original := TableSchema{
		Fields: []FieldSchema{
			{Name: "id", Type: BQInt64, Mode: ModeRequired},
			{Name: "name", Type: BQString},
			{Name: "active", Type: BQBool},
			{Name: "score", Type: BQFloat64},
			{Name: "created", Type: BQTimestamp},
			{Name: "data", Type: BQBytes},
			{Name: "birth", Type: BQDate},
		},
	}

	arrowSchema, err := BQSchemaToArrowSchema(original)
	if err != nil {
		t.Fatalf("BQSchemaToArrowSchema error: %v", err)
	}

	roundtrip := ArrowSchemaToTableSchema(arrowSchema)

	if len(roundtrip.Fields) != len(original.Fields) {
		t.Fatalf("expected %d fields, got %d", len(original.Fields), len(roundtrip.Fields))
	}

	for i, orig := range original.Fields {
		rt := roundtrip.Fields[i]
		if rt.Name != orig.Name {
			t.Errorf("field %d: expected name %q, got %q", i, orig.Name, rt.Name)
		}
		if rt.Type != orig.Type {
			t.Errorf("field %d (%s): expected type %q, got %q", i, orig.Name, orig.Type, rt.Type)
		}
	}
}

func TestBuildArrowRecord_BasicTypes(t *testing.T) {
	alloc := memory.NewGoAllocator()
	columns := []ColumnMeta{
		{Name: "id", Type: BQInt64},
		{Name: "name", Type: BQString},
		{Name: "active", Type: BQBool},
	}
	rows := [][]interface{}{
		{int64(1), "alice", true},
		{int64(2), "bob", false},
		{int64(3), "charlie", true},
	}

	record, err := BuildArrowRecord(alloc, columns, rows)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer record.Release()

	if record.NumCols() != 3 {
		t.Fatalf("expected 3 columns, got %d", record.NumCols())
	}
	if record.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", record.NumRows())
	}

	// Verify column names
	if record.ColumnName(0) != "id" {
		t.Errorf("expected column 0 name 'id', got %q", record.ColumnName(0))
	}
	if record.ColumnName(1) != "name" {
		t.Errorf("expected column 1 name 'name', got %q", record.ColumnName(1))
	}
	if record.ColumnName(2) != "active" {
		t.Errorf("expected column 2 name 'active', got %q", record.ColumnName(2))
	}

	// Verify int64 values
	col0 := record.Column(0)
	if col0.Len() != 3 {
		t.Errorf("expected 3 int64 values, got %d", col0.Len())
	}

	// Verify string values
	col1 := record.Column(1)
	if col1.Len() != 3 {
		t.Errorf("expected 3 string values, got %d", col1.Len())
	}
}

func TestBuildArrowRecord_WithNulls(t *testing.T) {
	alloc := memory.NewGoAllocator()
	columns := []ColumnMeta{
		{Name: "id", Type: BQInt64},
		{Name: "name", Type: BQString},
		{Name: "score", Type: BQFloat64},
	}
	rows := [][]interface{}{
		{int64(1), "alice", 95.5},
		{int64(2), nil, nil},
		{nil, "charlie", 87.3},
	}

	record, err := BuildArrowRecord(alloc, columns, rows)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer record.Release()

	if record.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", record.NumRows())
	}

	// Check null counts
	col0 := record.Column(0)
	if col0.NullN() != 1 {
		t.Errorf("expected 1 null in id column, got %d", col0.NullN())
	}

	col1 := record.Column(1)
	if col1.NullN() != 1 {
		t.Errorf("expected 1 null in name column, got %d", col1.NullN())
	}

	col2 := record.Column(2)
	if col2.NullN() != 1 {
		t.Errorf("expected 1 null in score column, got %d", col2.NullN())
	}
}

func TestBuildArrowRecord_Empty(t *testing.T) {
	alloc := memory.NewGoAllocator()
	columns := []ColumnMeta{
		{Name: "id", Type: BQInt64},
		{Name: "name", Type: BQString},
	}
	rows := [][]interface{}{}

	record, err := BuildArrowRecord(alloc, columns, rows)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer record.Release()

	if record.NumCols() != 2 {
		t.Fatalf("expected 2 columns, got %d", record.NumCols())
	}
	if record.NumRows() != 0 {
		t.Fatalf("expected 0 rows, got %d", record.NumRows())
	}
}

func TestBuildArrowRecord_TypeCoercion(t *testing.T) {
	// DuckDB may return int32 for int64 columns, float32 for float64, []byte for string
	alloc := memory.NewGoAllocator()
	columns := []ColumnMeta{
		{Name: "id", Type: BQInt64},
		{Name: "score", Type: BQFloat64},
		{Name: "name", Type: BQString},
		{Name: "ts", Type: BQTimestamp},
	}

	now := time.Now()
	rows := [][]interface{}{
		{int32(42), float32(3.14), []byte("hello"), now},
	}

	record, err := BuildArrowRecord(alloc, columns, rows)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer record.Release()

	if record.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", record.NumRows())
	}
	if record.NumCols() != 4 {
		t.Fatalf("expected 4 columns, got %d", record.NumCols())
	}
}

func BenchmarkBuildArrowRecord_1K(b *testing.B) {
	alloc := memory.NewGoAllocator()
	columns := []ColumnMeta{
		{Name: "id", Type: BQInt64},
		{Name: "name", Type: BQString},
		{Name: "active", Type: BQBool},
		{Name: "score", Type: BQFloat64},
	}

	// Build 1000 rows
	rows := make([][]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		rows[i] = []interface{}{int64(i), "user_name", true, 99.5}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, err := BuildArrowRecord(alloc, columns, rows)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
		record.Release()
	}
}
