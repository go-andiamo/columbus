package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-andiamo/columbus"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
	"time"
)

const testTable = `test_table`

var db *sql.DB
var ctx = context.Background()

func TestMain(m *testing.M) {
	db = createTestDatabase(cfg)
	defer func() {
		_ = db.Close()
	}()
	code := m.Run()
	os.Exit(code)
}

func TestPostgres(t *testing.T) {
	_, err := db.Exec(`DELETE FROM ` + testTable)
	require.NoError(t, err)
	insert(db, map[string]any{
		"col_a": nil,
		"col_b": "bbb",
		"col_c": time.Now(),
		"col_d": "TEXT",
		"col_e": `{"foo":"bar"}`,
		"col_f": `{"foo":"bar"}`,
		"col_g": 16.16,
		"col_h": true,
		"col_i": 16,
		"col_j": 16.16,
		"col_u": "216b5e48-5af9-4a05-800b-b5e264a74b7e",
	})

	const cols = `*`
	scanners := columbus.DbTypeScanners{
		"UUID": func(src any) (value any, err error) {
			switch st := src.(type) {
			case []byte:
				return uuid.Parse(string(st))
			case string:
				return uuid.Parse(st)
			}
			return nil, fmt.Errorf("unexpected type %T", src)
		},
	}
	mapper, err := columbus.NewMapper(cols,
		//columbus.Mappings{"col_h": {Scanner: columbus.BoolColumn}},
		columbus.Query(`FROM `+testTable),
		scanners,
	)
	require.NoError(t, err)
	require.NotNil(t, mapper)

	row, err := mapper.FirstRow(ctx, db, nil)
	require.NoError(t, err)
	require.NotNil(t, row)

	require.Nil(t, row["col_a"])
	require.Equal(t, "bbb", row["col_b"])
	require.IsType(t, time.Time{}, row["col_c"])
	require.Equal(t, "TEXT", row["col_d"])
	require.Equal(t, map[string]any{"foo": "bar"}, row["col_e"])
	require.True(t, row["col_g"].(decimal.Decimal).Equal(decimal.NewFromFloat(16.16)))
	require.Equal(t, true, row["col_h"])
	require.Equal(t, int64(16), row["col_i"])
	require.Equal(t, "16.16", row["col_j"].(decimal.Decimal).StringFixed(2))
	require.IsType(t, uuid.UUID{}, row["col_u"])
	require.Equal(t, "216b5e48-5af9-4a05-800b-b5e264a74b7e", row["col_u"].(uuid.UUID).String())

	subq := columbus.NewSubQuery("sub", `SELECT * FROM `+testTable, nil, nil, false)
	excf := func(property string, path []string) bool {
		if len(path) == 1 && path[0] == "sub" {
			return false
		}
		return property != "col_b" && property != "sub"
	}
	row, err = mapper.FirstRow(ctx, db, nil, subq, excf)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, 2, len(row))
	require.Equal(t, "bbb", row["col_b"])
	require.NotEmpty(t, row["sub"])
}

func TestPostgres_StructMapping(t *testing.T) {
	_, err := db.Exec(`DELETE FROM ` + testTable)
	require.NoError(t, err)
	insert(db, map[string]any{
		"col_a": nil,
		"col_b": "bbb",
		"col_c": time.Now(),
		"col_d": "TEXT",
		//		"col_e": `{"foo":"bar"}`,
		"col_e": nil,
		"col_f": `[{"foo":"bar"}]`,
		"col_g": 16.16,
		"col_h": true,
		"col_i": 16,
		"col_j": 16.16,
		"col_u": "216b5e48-5af9-4a05-800b-b5e264a74b7e",
	})

	type Row struct {
		ColA sql.NullString      `sql:"col_a"`
		ColB sql.NullString      `sql:"col_b"`
		ColC sql.NullTime        `sql:"col_c"`
		ColD sql.NullString      `sql:"col_d"`
		ColE JsonCol             `sql:"col_e"`
		ColF JsonCol             `sql:"col_f"`
		ColG decimal.NullDecimal `sql:"col_g"`
		ColH sql.NullBool        `sql:"col_h"`
		ColI any                 `sql:"col_i"`
		ColJ decimal.NullDecimal `sql:"col_j"`
		ColK any                 `sql:"col_k"`
		ColM any                 `sql:"col_m"`
		ColN any                 `sql:"col_n"`
		ColU uuid.UUID           `sql:"col_u"`
	}
	sm := columbus.MustNewStructMapper[Row]("*", columbus.Query(`FROM `+testTable))
	row, err := sm.FirstRow(ctx, db, nil)
	require.NoError(t, err)
	require.NotNil(t, row)
}

type JsonCol struct {
	Value any
}

func (j *JsonCol) Scan(src any) error {
	if data, ok := src.([]byte); ok {
		var v any
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		j.Value = v
	} else if src != nil {
		return fmt.Errorf("unexpected type %T", src)
	}
	return nil
}

var cfg = config{
	Host:     "localhost",
	Port:     5432,
	Username: "root",
	Password: "root",
	Name:     "test_db",
}

type config struct {
	Host     string
	Port     int
	Username string
	Password string
	Name     string
}

func insert(db *sql.DB, row map[string]any) {
	cols := make([]string, 0, len(row))
	args := make([]any, 0, len(row))
	markers := make([]string, 0, len(row))
	arg := 1
	for k, v := range row {
		cols = append(cols, k)
		args = append(args, v)
		markers = append(markers, fmt.Sprintf("$%d", arg))
		arg++
	}
	query := `INSERT INTO ` + testTable + ` (` + strings.Join(cols, ",") + `) VALUES (` + strings.Join(markers, ",") + `)`
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		panic(err)
	}
}

func createTestTable(db *sql.DB) {
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (
    	col_a CHAR(20),
    	col_b VARCHAR(20),
    	col_c TIMESTAMP(3)  DEFAULT CURRENT_TIMESTAMP(3),
    	col_d TEXT,
    	col_e JSON,
    	col_f JSONB,
    	col_g DECIMAL(20,3),
    	col_h BOOLEAN,
    	col_i INT,
    	col_j FLOAT,
    	col_k BIGINT,
    	col_m REAL,
    	col_n BIT,
    	col_u UUID
    )`, testTable))
	if err != nil {
		if pgErr, ok := err.(*pq.Error); ok {
			if pgErr.Code != "42P07" {
				panic(err)
			}
		} else {
			panic(err)
		}
	}
}

func createTestDatabase(cfg config) *sql.DB {
	db, err := sql.Open("postgres", dsn(cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Name))
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if pgErr, ok := err.(*pq.Error); ok && pgErr.Code == "3D000" {
		// database (catalog) does not exist - try creating it...
		db = createDatabase(cfg)
	}
	createTestTable(db)
	return db
}

func createDatabase(cfg config) *sql.DB {
	if db, err := sql.Open("postgres", dsn(cfg.Host, cfg.Port, cfg.Username, cfg.Password, "postgres")); err == nil {
		if _, err = db.Exec("CREATE DATABASE " + cfg.Name); err == nil {
			_ = db.Close()
			if db, err = sql.Open("postgres", dsn(cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Name)); err == nil {
				if err = db.Ping(); err != nil {
					panic(err)
				}
				return db
			} else {
				panic(err)
			}
		} else {
			panic(err)
		}
	} else {
		panic(err)
	}
}

func dsn(host string, port int, username, password, dbName string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		username, password, host, port, dbName)
}
