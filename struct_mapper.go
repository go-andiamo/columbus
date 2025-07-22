package columbus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

const sqlTag = "sql"

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// UseTagName is a type that can be passed as an option to NewStructMapper
// and determines the field tag name to use for field column mappings
//
// If this option is not passed to NewStructMapper, then the default "sql" tag is used
type UseTagName string

// FieldColumnNamer is an interface that can be passed as an option to NewStructMapper
// and is used to derive the column name to use for a given field
//
// If this option is not specified (or none are satisfied), the name is deduced from the "sql" tag for the field
type FieldColumnNamer interface {
	// ColumnName returns the column name to use for the given struct field
	//
	// The returned name is only used if second return arg is true
	ColumnName(structType reflect.Type, fld reflect.StructField) (string, bool)
}

// ErrorOnUnknownColumns is a type that can be passed as an option to NewStructMapper
// and determines whether an error is raised when a field is mapped, by tag, to an unknown column
type ErrorOnUnknownColumns bool

// ErrorOnUnMappedColumns is a type that can be passed as an option to NewStructMapper
// and determines whether an error is raised when there are columns that are not mapped to fields
type ErrorOnUnMappedColumns bool

// StructPostProcessor is an interface that can be passed as an option to NewStructMapper (or
// any of the row reading methods - StructMapper.Rows, StructMapper.Iterate, StructMapper.FirstRow, StructMapper.ExactlyOneRow, etc.)
//
// Multiple StructPostProcessor can be used, each one is called sequentially
type StructPostProcessor[T any] interface {
	// PostProcess executes the StructPostProcessor
	PostProcess(ctx context.Context, db SqlInterface, row *T) error
}

// StructMapper is the interface returned by NewStructMapper / MustNewStructMapper
type StructMapper[T any] interface {
	// Rows reads all rows and maps them into a slice of `T`
	//
	// options can be any of Query, AddClause, StructPostProcessor[T], ErrorTranslator or Limiter
	Rows(ctx context.Context, db SqlInterface, args []any, options ...any) ([]T, error)
	// Iterate iterates over the rows and calls the supplied handler with each row
	//
	// iteration stops at the end of rows - or an error is encountered - or the supplied handler returns false for `cont` (continue)
	//
	// options can be any of Query, AddClause, StructPostProcessor[T], ErrorTranslator or Limiter (ignored)
	Iterate(ctx context.Context, db SqlInterface, args []any, handler func(row T) (cont bool, err error), options ...any) error
	// Iterator return an iterator that can be ranged over
	//
	// options can be any of Query, AddClause, StructPostProcessor[T], ErrorTranslator or Limiter
	Iterator(ctx context.Context, db SqlInterface, args []any, options ...any) func(func(int, T) bool)
	// FirstRow reads just the first row and maps it into a `T`
	//
	// if there are no rows, returns nil
	//
	// options can be any of Query, AddClause, StructPostProcessor[T], ErrorTranslator or Limiter (ignored)
	FirstRow(ctx context.Context, sqli SqlInterface, args []any, options ...any) (*T, error)
	// ExactlyOneRow reads exactly one row and maps it into a `T`
	//
	// if there are no rows, returns error sql.ErrNoRows
	//
	// options can be any of Query, AddClause, StructPostProcessor[T], ErrorTranslator or Limiter (ignored)
	ExactlyOneRow(ctx context.Context, sqli SqlInterface, args []any, options ...any) (T, error)
}

type structMapper[T any] struct {
	cols                   string
	defaultQuery           *Query
	mu                     sync.RWMutex
	mapped                 bool
	fieldMappers           func(*T) []any
	errorOnUnknownColumns  bool
	errorOnUnMappedColumns bool
	mapError               error
	postProcessors         []StructPostProcessor[T]
	useTagName             string
	fieldColumnNamers      []FieldColumnNamer
	errorTranslator        ErrorTranslator
}

// NewStructMapper creates a new struct mapper for reading structs from database rows
func NewStructMapper[T any](cols string, options ...any) (StructMapper[T], error) {
	var zero T
	if reflect.TypeOf(zero).Kind() != reflect.Struct {
		return nil, errors.New("StructMapper can only be used with struct types")
	}
	return (&structMapper[T]{
		cols:            cols,
		errorTranslator: defaultErrorTranslator,
	}).processInitialOptions(options)
}

// MustNewStructMapper is the same as NewStructMapper except that it panics on error
func MustNewStructMapper[T any](cols string, options ...any) StructMapper[T] {
	result, err := NewStructMapper[T](cols, options...)
	if err != nil {
		panic(err)
	}
	return result
}

func (m *structMapper[T]) Rows(ctx context.Context, db SqlInterface, args []any, options ...any) (result []T, err error) {
	query, postProcessors, limiter, errTranslator, err := m.rowMapOptions(options)
	if err == nil {
		var rows *sql.Rows
		if rows, err = db.QueryContext(ctx, query, args...); err == nil {
			defer func() {
				_ = rows.Close()
			}()
			var fieldPtrs func(*T) []any
			if fieldPtrs, err = m.getFieldMappers(rows); err == nil {
				rowCount := 0
				for err == nil && rows.Next() {
					rowCount++
					if limiter.LimitReached(rowCount) {
						break
					}
					var item T
					if err = rows.Scan(fieldPtrs(&item)...); err == nil {
						for _, pp := range postProcessors {
							if err = pp.PostProcess(ctx, db, &item); err != nil {
								return nil, translateError(err, errTranslator)
							}
						}
						result = append(result, item)
					}
				}
				if err == nil {
					err = rows.Err()
				}
			}
		}
	}
	return result, translateError(err, errTranslator)
}

func (m *structMapper[T]) Iterate(ctx context.Context, db SqlInterface, args []any, handler func(row T) (cont bool, err error), options ...any) (err error) {
	query, postProcessors, _, errTranslator, err := m.rowMapOptions(options)
	if err == nil {
		var rows *sql.Rows
		if rows, err = db.QueryContext(ctx, query, args...); err == nil {
			defer func() {
				_ = rows.Close()
			}()
			var fieldPtrs func(*T) []any
			if fieldPtrs, err = m.getFieldMappers(rows); err == nil {
				cont := true
				for cont && err == nil && rows.Next() {
					var item T
					if err = rows.Scan(fieldPtrs(&item)...); err == nil {
						for _, pp := range postProcessors {
							if err = pp.PostProcess(ctx, db, &item); err != nil {
								return translateError(err, errTranslator)
							}
						}
						cont, err = handler(item)
					}
				}
				if err == nil {
					err = rows.Err()
				}
			}
		}
	}
	return translateError(err, errTranslator)
}

func (m *structMapper[T]) Iterator(ctx context.Context, db SqlInterface, args []any, options ...any) func(func(int, T) bool) {
	query, postProcessors, limiter, errTranslator, err := m.rowMapOptions(options)
	if err == nil {
		i := 0
		var rows *sql.Rows
		if rows, err = db.QueryContext(ctx, query, args...); err == nil {
			return func(yield func(int, T) bool) {
				var fieldPtrs func(*T) []any
				if fieldPtrs, err = m.getFieldMappers(rows); err == nil {
					for err == nil && rows.Next() {
						if limiter.LimitReached(i + 1) {
							break
						}
						var item T
						if err = rows.Scan(fieldPtrs(&item)...); err == nil {
							for _, pp := range postProcessors {
								if err = pp.PostProcess(ctx, db, &item); err != nil {
									break
								}
							}
							if err == nil {
								yield(i, item)
							} else {
								err = translateError(err, errTranslator)
							}
							i++
						}
					}
				}
				_ = rows.Close()
				if err != nil {
					_ = translateError(err, errTranslator)
				}
			}
		}
	}
	_ = translateError(err, errTranslator)
	return func(func(int, T) bool) {}
}

func (m *structMapper[T]) FirstRow(ctx context.Context, sqli SqlInterface, args []any, options ...any) (result *T, err error) {
	query, postProcessors, _, errTranslator, err := m.rowMapOptions(options)
	if err == nil {
		var rows *sql.Rows
		if rows, err = sqli.QueryContext(ctx, query, args...); err == nil {
			defer func() {
				_ = rows.Close()
			}()
			var fieldPtrs func(*T) []any
			if fieldPtrs, err = m.getFieldMappers(rows); err == nil {
				if rows.Next() {
					var item T
					if err = rows.Scan(fieldPtrs(&item)...); err == nil {
						for _, pp := range postProcessors {
							if err = pp.PostProcess(ctx, sqli, &item); err != nil {
								return nil, translateError(err, errTranslator)
							}
						}
						result = &item
					}
				}
			}
		}
	}
	return result, translateError(err, errTranslator)
}

func (m *structMapper[T]) ExactlyOneRow(ctx context.Context, sqli SqlInterface, args []any, options ...any) (result T, err error) {
	query, postProcessors, _, errTranslator, err := m.rowMapOptions(options)
	if err == nil {
		var rows *sql.Rows
		if rows, err = sqli.QueryContext(ctx, query, args...); err == nil {
			defer func() {
				_ = rows.Close()
			}()
			var fieldPtrs func(*T) []any
			if fieldPtrs, err = m.getFieldMappers(rows); err == nil {
				if rows.Next() {
					if err = rows.Scan(fieldPtrs(&result)...); err == nil {
						for _, pp := range postProcessors {
							if err = pp.PostProcess(ctx, sqli, &result); err != nil {
								return result, translateError(err, errTranslator)
							}
						}
					}
				} else {
					err = sql.ErrNoRows
				}
			}
		}
	}
	return result, translateError(err, errTranslator)
}

func (m *structMapper[T]) processInitialOptions(options []any) (StructMapper[T], error) {
	m.useTagName = sqlTag
	seenQuery := false
	for _, o := range options {
		if o != nil {
			switch option := o.(type) {
			case Query:
				if seenQuery {
					return nil, errors.New("cannot use multiple default queries")
				}
				seenQuery = true
				if err := checkForgedColumns(option); err != nil {
					return nil, err
				}
				qStr := Query("SELECT " + m.cols + " " + string(option))
				m.defaultQuery = &qStr
			case ErrorOnUnknownColumns:
				m.errorOnUnknownColumns = bool(option)
			case ErrorOnUnMappedColumns:
				m.errorOnUnMappedColumns = bool(option)
			case StructPostProcessor[T]:
				m.postProcessors = append(m.postProcessors, option)
			case UseTagName:
				if option != "" {
					m.useTagName = string(option)
				}
			case FieldColumnNamer:
				m.fieldColumnNamers = append(m.fieldColumnNamers, option)
			case ErrorTranslator:
				m.errorTranslator = option
			default:
				return nil, fmt.Errorf("unknown option type: %T", o)
			}
		}
	}
	m.fieldColumnNamers = append(m.fieldColumnNamers, &defaultFieldColumnNamer{tagName: m.useTagName})
	if err := m.checkDuplicateMappedColumns(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *structMapper[T]) rowMapOptions(options []any) (query string, postProcessors []StructPostProcessor[T], limiter Limiter, errorTranslator ErrorTranslator, err error) {
	querySet := false
	postProcessors = append(postProcessors, m.postProcessors...)
	limiter = defaultLimiter
	errorTranslator = m.errorTranslator
	var qb strings.Builder
	if m.defaultQuery != nil {
		querySet = true
		qb.WriteString(string(*m.defaultQuery))
	}
	for _, o := range options {
		if o != nil {
			switch option := o.(type) {
			case Query:
				querySet = true
				qb.Reset()
				if err = checkForgedColumns(option); err != nil {
					return
				}
				qb.WriteString("SELECT " + m.cols + " " + string(option))
			case AddClause:
				if !querySet {
					err = errors.New("add clause must have a query set")
					return
				}
				qb.WriteString(" " + string(option))
			case StructPostProcessor[T]:
				postProcessors = append(postProcessors, option)
			case Limiter:
				limiter = option
			case ErrorTranslator:
				errorTranslator = option
			default:
				err = fmt.Errorf("unknown option type: %T", o)
				return
			}
		}
	}
	if !querySet {
		err = errors.New("no default query")
	}
	return qb.String(), postProcessors, limiter, errorTranslator, err
}

func checkForgedColumns(query Query) error {
	if strings.HasPrefix(strings.TrimLeft(string(query), " \t\r\n"), ",") {
		return errors.New("cannot forge extra columns using Query")
	}
	return nil
}

func (m *structMapper[T]) getFieldMappers(rows *sql.Rows) (func(*T) []any, error) {
	m.mu.RLock()
	if m.mapped {
		m.mu.RUnlock()
		return m.fieldMappers, m.mapError
	}
	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	var err error
	var columns []string
	if columns, err = rows.Columns(); err == nil {
		var columnMap map[string]func(any) any
		var knownCols map[string]bool
		if columnMap, knownCols, err = m.mapColumns(columns); err == nil {
			m.mapped = true
			if m.errorOnUnMappedColumns {
				unmapped := make([]string, 0, len(knownCols))
				for col, mapped := range knownCols {
					if !mapped {
						unmapped = append(unmapped, col)
					}
				}
				if len(unmapped) > 0 {
					m.mapError = fmt.Errorf("unmapped column(s): %s", `"`+strings.Join(unmapped, `","`)+`"`)
					return nil, m.mapError
				}
			}
			if m.errorOnUnknownColumns {
				unknown := make([]string, 0, len(columnMap))
				for k := range columnMap {
					if _, ok := knownCols[k]; !ok {
						unknown = append(unknown, k)
					}
				}
				if len(unknown) > 0 {
					m.mapError = fmt.Errorf("unknown column(s): %s", `"`+strings.Join(unknown, `","`)+`"`)
					return nil, m.mapError
				}
			}
			m.fieldMappers = func(t *T) []any {
				ptrs := make([]any, len(columns))
				for i, col := range columns {
					if acc, ok := columnMap[col]; ok {
						ptrs[i] = acc(t)
					} else {
						var discard any
						ptrs[i] = &discard
					}
				}
				return ptrs
			}
		}
	}
	return m.fieldMappers, err
}

func (m *structMapper[T]) mapColumns(columns []string) (map[string]func(any) any, map[string]bool, error) {
	rt := reflect.TypeOf((*T)(nil)).Elem()
	knownCols := make(map[string]bool, len(columns))
	for _, col := range columns {
		knownCols[col] = false
	}
	result := make(map[string]func(any) any)
	err := buildFieldMapRecursive(m.fieldColumnNamers, rt, nil, result, knownCols)
	return result, knownCols, err
}

func buildFieldMapRecursive(namers []FieldColumnNamer, rt reflect.Type, parentIndex []int, result map[string]func(any) any, knownCols map[string]bool) (err error) {
	for i := 0; err == nil && i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() {
			continue
		}
		index := append([]int{}, parentIndex...)
		index = append(index, f.Index...)
		if f.Type.Kind() == reflect.Struct && !isScannable(f.Type) {
			err = buildFieldMapRecursive(namers, f.Type, index, result, knownCols)
			continue
		}
		useColName := ""
		named := false
		for _, namer := range namers {
			if useColName, named = namer.ColumnName(rt, f); named {
				break
			}
		}
		if !named || useColName == "-" || useColName == "" {
			continue
		}
		if _, ok := knownCols[useColName]; ok {
			knownCols[useColName] = true
		}
		indexCopy := append([]int{}, index...)
		result[useColName] = func(obj any) any {
			return reflect.ValueOf(obj).Elem().FieldByIndex(indexCopy).Addr().Interface()
		}
	}
	return err
}

func isScannable(t reflect.Type) bool {
	if t == nil {
		return false
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return true
	}
	// bizarrely, time.Time isn't scannable but drivers can scan it...
	if t.PkgPath() == "time" && t.Name() == "Time" {
		return true
	}
	return t.Implements(scannerType) || reflect.PointerTo(t).Implements(scannerType)
}

func (m *structMapper[T]) checkDuplicateMappedColumns() error {
	rt := reflect.TypeOf((*T)(nil)).Elem()
	return walkStruct(m.fieldColumnNamers, rt, make(map[string]struct{}))
}

func walkStruct(namers []FieldColumnNamer, rt reflect.Type, seen map[string]struct{}) error {
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() {
			continue
		}
		t := f.Type
		for t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if t.Kind() == reflect.Struct && !isScannable(t) {
			if err := walkStruct(namers, t, seen); err != nil {
				return err
			}
			continue
		}
		useColName := ""
		named := false
		for _, namer := range namers {
			if useColName, named = namer.ColumnName(rt, f); named {
				break
			}
		}
		if !named || useColName == "-" || useColName == "" {
			continue
		}
		if _, exists := seen[useColName]; exists {
			return fmt.Errorf("duplicate column mapping %q", useColName)
		}
		seen[useColName] = struct{}{}
	}
	return nil
}

type defaultFieldColumnNamer struct {
	tagName string
}

var _ FieldColumnNamer = &defaultFieldColumnNamer{}

func (d *defaultFieldColumnNamer) ColumnName(structType reflect.Type, fld reflect.StructField) (string, bool) {
	tag, ok := fld.Tag.Lookup(d.tagName)
	if !ok || tag == "-" || tag == "" {
		return "", false
	}
	return tag, true
}
