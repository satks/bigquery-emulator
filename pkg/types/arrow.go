package types

import (
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// bqTypeToArrowType maps a BigQuery type string to an Arrow DataType.
// Returns nil and an error for unknown types.
func bqTypeToArrowType(bqType string) (arrow.DataType, error) {
	switch strings.ToUpper(bqType) {
	case BQInt64:
		return arrow.PrimitiveTypes.Int64, nil
	case BQFloat64:
		return arrow.PrimitiveTypes.Float64, nil
	case BQBool:
		return arrow.FixedWidthTypes.Boolean, nil
	case BQString:
		return arrow.BinaryTypes.String, nil
	case BQBytes:
		return arrow.BinaryTypes.Binary, nil
	case BQDate:
		return arrow.FixedWidthTypes.Date32, nil
	case BQTime:
		return arrow.FixedWidthTypes.Time64us, nil
	case BQTimestamp, BQDatetime:
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case BQNumeric, BQBigNumeric:
		return arrow.BinaryTypes.String, nil // store as string representation
	case BQJson:
		return arrow.BinaryTypes.String, nil
	case BQGeography:
		return arrow.BinaryTypes.String, nil
	case BQInterval:
		return arrow.BinaryTypes.String, nil
	default:
		return nil, fmt.Errorf("unsupported BigQuery type: %s", bqType)
	}
}

// arrowTypeToVQType maps an Arrow DataType back to a BigQuery type string.
func arrowTypeToBQType(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.INT64:
		return BQInt64
	case arrow.FLOAT64:
		return BQFloat64
	case arrow.BOOL:
		return BQBool
	case arrow.STRING, arrow.LARGE_STRING:
		return BQString
	case arrow.BINARY, arrow.LARGE_BINARY:
		return BQBytes
	case arrow.DATE32, arrow.DATE64:
		return BQDate
	case arrow.TIME64:
		return BQTime
	case arrow.TIMESTAMP:
		return BQTimestamp
	case arrow.STRUCT:
		return BQStruct
	case arrow.LIST:
		return BQArray
	default:
		return BQString // safe fallback
	}
}

// BQFieldToArrowField converts a BigQuery FieldSchema to an Arrow field.
func BQFieldToArrowField(field FieldSchema) (arrow.Field, error) {
	nullable := strings.ToUpper(field.Mode) != ModeRequired

	// Handle STRUCT/RECORD with nested fields
	upperType := strings.ToUpper(field.Type)
	if (upperType == BQStruct || upperType == BQRecord) && len(field.Fields) > 0 {
		nestedFields := make([]arrow.Field, len(field.Fields))
		for i, f := range field.Fields {
			af, err := BQFieldToArrowField(f)
			if err != nil {
				return arrow.Field{}, fmt.Errorf("nested field %q: %w", f.Name, err)
			}
			nestedFields[i] = af
		}
		structType := arrow.StructOf(nestedFields...)
		// Handle REPEATED STRUCT
		if strings.ToUpper(field.Mode) == ModeRepeated {
			return arrow.Field{
				Name:     field.Name,
				Type:     arrow.ListOf(structType),
				Nullable: true,
			}, nil
		}
		return arrow.Field{
			Name:     field.Name,
			Type:     structType,
			Nullable: nullable,
		}, nil
	}

	// Get base Arrow type
	arrowType, err := bqTypeToArrowType(field.Type)
	if err != nil {
		return arrow.Field{}, err
	}

	// Handle REPEATED mode -> ListOf
	if strings.ToUpper(field.Mode) == ModeRepeated {
		return arrow.Field{
			Name:     field.Name,
			Type:     arrow.ListOf(arrowType),
			Nullable: true,
		}, nil
	}

	return arrow.Field{
		Name:     field.Name,
		Type:     arrowType,
		Nullable: nullable,
	}, nil
}

// BQSchemaToArrowSchema converts a BigQuery TableSchema to an Arrow schema.
func BQSchemaToArrowSchema(schema TableSchema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(schema.Fields))
	for i, f := range schema.Fields {
		af, err := BQFieldToArrowField(f)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.Name, err)
		}
		fields[i] = af
	}
	return arrow.NewSchema(fields, nil), nil
}

// ArrowFieldToBQField converts an Arrow field back to a BigQuery FieldSchema.
func ArrowFieldToBQField(field arrow.Field) FieldSchema {
	fs := FieldSchema{
		Name: field.Name,
	}

	// Check for LIST (REPEATED)
	if listType, ok := field.Type.(*arrow.ListType); ok {
		fs.Mode = ModeRepeated
		elemType := listType.Elem()
		// Check if list element is a struct
		if structType, ok := elemType.(*arrow.StructType); ok {
			fs.Type = BQRecord
			fs.Fields = make([]FieldSchema, structType.NumFields())
			for i := 0; i < structType.NumFields(); i++ {
				fs.Fields[i] = ArrowFieldToBQField(structType.Field(i))
			}
		} else {
			fs.Type = arrowTypeToBQType(elemType)
		}
		return fs
	}

	// Check for STRUCT
	if structType, ok := field.Type.(*arrow.StructType); ok {
		fs.Type = BQRecord
		fs.Fields = make([]FieldSchema, structType.NumFields())
		for i := 0; i < structType.NumFields(); i++ {
			fs.Fields[i] = ArrowFieldToBQField(structType.Field(i))
		}
		if !field.Nullable {
			fs.Mode = ModeRequired
		}
		return fs
	}

	// Base type
	fs.Type = arrowTypeToBQType(field.Type)
	if !field.Nullable {
		fs.Mode = ModeRequired
	}
	return fs
}

// ArrowSchemaToTableSchema converts an Arrow schema to BigQuery TableSchema.
func ArrowSchemaToTableSchema(schema *arrow.Schema) TableSchema {
	fields := make([]FieldSchema, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		fields[i] = ArrowFieldToBQField(schema.Field(i))
	}
	return TableSchema{Fields: fields}
}

// columnMetaToArrowField converts query result ColumnMeta to an Arrow field.
func columnMetaToArrowField(col ColumnMeta) (arrow.Field, error) {
	arrowType, err := bqTypeToArrowType(col.Type)
	if err != nil {
		// Fallback to string for unknown types
		arrowType = arrow.BinaryTypes.String
	}
	return arrow.Field{
		Name:     col.Name,
		Type:     arrowType,
		Nullable: true, // query results are always nullable
	}, nil
}

// BuildArrowRecord converts query result rows to an Arrow record batch.
// Takes column metadata and row data from QueryResult.
func BuildArrowRecord(alloc memory.Allocator, columns []ColumnMeta, rows [][]interface{}) (arrow.Record, error) {
	// Build Arrow schema from column metadata
	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		f, err := columnMetaToArrowField(col)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		fields[i] = f
	}
	schema := arrow.NewSchema(fields, nil)

	// Create builders for each column
	builders := make([]array.Builder, len(columns))
	for i, f := range fields {
		builders[i] = builderForType(alloc, f.Type)
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	// Append row values to builders
	for rowIdx, row := range rows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("row %d has %d values, expected %d", rowIdx, len(row), len(columns))
		}
		for colIdx, val := range row {
			if err := appendValue(builders[colIdx], columns[colIdx].Type, val); err != nil {
				return nil, fmt.Errorf("row %d, col %d (%s): %w", rowIdx, colIdx, columns[colIdx].Name, err)
			}
		}
	}

	// Build arrays and create record
	arrays := make([]arrow.Array, len(builders))
	for i, b := range builders {
		arrays[i] = b.NewArray()
	}
	defer func() {
		for _, a := range arrays {
			a.Release()
		}
	}()

	record := array.NewRecord(schema, arrays, int64(len(rows)))
	return record, nil
}

// builderForType returns the appropriate Arrow array builder for a given type.
func builderForType(alloc memory.Allocator, dt arrow.DataType) array.Builder {
	switch dt.ID() {
	case arrow.INT64:
		return array.NewInt64Builder(alloc)
	case arrow.FLOAT64:
		return array.NewFloat64Builder(alloc)
	case arrow.BOOL:
		return array.NewBooleanBuilder(alloc)
	case arrow.STRING:
		return array.NewStringBuilder(alloc)
	case arrow.BINARY:
		return array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
	case arrow.DATE32:
		return array.NewDate32Builder(alloc)
	case arrow.TIME64:
		return array.NewTime64Builder(alloc, arrow.FixedWidthTypes.Time64us.(*arrow.Time64Type))
	case arrow.TIMESTAMP:
		tsType := dt.(*arrow.TimestampType)
		return array.NewTimestampBuilder(alloc, tsType)
	default:
		return array.NewStringBuilder(alloc) // fallback to string
	}
}

// appendValue appends a single value to the appropriate builder, handling type coercion.
func appendValue(builder array.Builder, bqType string, val interface{}) error {
	if val == nil {
		builder.AppendNull()
		return nil
	}

	upperType := strings.ToUpper(bqType)

	switch upperType {
	case BQInt64:
		b := builder.(*array.Int64Builder)
		switch v := val.(type) {
		case int64:
			b.Append(v)
		case int32:
			b.Append(int64(v))
		case int:
			b.Append(int64(v))
		default:
			b.Append(0) // fallback
		}

	case BQFloat64:
		b := builder.(*array.Float64Builder)
		switch v := val.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		case int64:
			b.Append(float64(v))
		default:
			b.Append(0) // fallback
		}

	case BQBool:
		b := builder.(*array.BooleanBuilder)
		switch v := val.(type) {
		case bool:
			b.Append(v)
		default:
			b.Append(false) // fallback
		}

	case BQString, BQJson, BQGeography, BQNumeric, BQBigNumeric, BQInterval:
		b := builder.(*array.StringBuilder)
		switch v := val.(type) {
		case string:
			b.Append(v)
		case []byte:
			b.Append(string(v))
		default:
			b.Append(fmt.Sprint(v))
		}

	case BQBytes:
		b := builder.(*array.BinaryBuilder)
		switch v := val.(type) {
		case []byte:
			b.Append(v)
		case string:
			b.Append([]byte(v))
		default:
			b.Append([]byte(fmt.Sprint(v)))
		}

	case BQDate:
		b := builder.(*array.Date32Builder)
		switch v := val.(type) {
		case time.Time:
			// Days since epoch
			days := int32(v.Unix() / 86400)
			b.Append(arrow.Date32(days))
		default:
			b.Append(0)
		}

	case BQTime:
		b := builder.(*array.Time64Builder)
		switch v := val.(type) {
		case time.Time:
			// Microseconds since midnight
			midnight := time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, v.Location())
			us := v.Sub(midnight).Microseconds()
			b.Append(arrow.Time64(us))
		default:
			b.Append(0)
		}

	case BQTimestamp, BQDatetime:
		b := builder.(*array.TimestampBuilder)
		switch v := val.(type) {
		case time.Time:
			b.Append(arrow.Timestamp(v.UnixMicro()))
		default:
			b.Append(0)
		}

	default:
		// Fallback: treat as string
		b, ok := builder.(*array.StringBuilder)
		if ok {
			b.Append(fmt.Sprint(val))
		} else {
			builder.AppendNull()
		}
	}

	return nil
}
