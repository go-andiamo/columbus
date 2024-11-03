package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-andiamo/columbus"
	"github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

const testTable = `test_table`

func TestMysql(t *testing.T) {
	db := createTestDatabase(cfg)
	defer func() {
		_ = db.Close()
	}()
	insert(db, map[string]any{
		"col_a": nil,
		"col_b": "bbb",
		"col_c": time.Now(),
		"col_d": "TEXT",
		"col_e": `{"foo":"bar"}`,
		"col_g": 16.16,
		"col_h": true,
		"col_i": 16,
		"col_j": 16.16,
	})

	const cols = `*`
	mapper, err := columbus.NewMapper(cols, columbus.Mappings{"col_h": {Scanner: columbus.BoolColumn}}, columbus.Query(`FROM `+testTable))
	require.NoError(t, err)
	require.NotNil(t, mapper)

	ctx := context.Background()
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

var cfg = config{
	Host:     "localhost",
	Port:     52717,
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
	for k, v := range row {
		cols = append(cols, k)
		args = append(args, v)
		markers = append(markers, "?")
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
    	col_g DECIMAL(20,3),
    	col_h BOOL,
    	col_i INT,
    	col_j FLOAT,
    	col_k BIGINT,
    	col_l TINYINT,
    	col_m REAL,
    	col_n BIT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, testTable))
	if err != nil {
		if myerr, ok := err.(*mysql.MySQLError); ok {
			if myerr.Number != 1050 {
				panic(err)
			}
		} else {
			panic(err)
		}
	}
}

func createTestDatabase(cfg config) *sql.DB {
	db, err := sql.Open("mysql", dsn(cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Name))
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if myErr, ok := err.(*mysql.MySQLError); ok && myErr.Number == 1049 {
		// database (catalog) does not exist - try creating it...
		db = createDatabase(cfg)
	}
	createTestTable(db)
	return db
}

func createDatabase(cfg config) *sql.DB {
	if db, err := sql.Open("mysql", dsn(cfg.Host, cfg.Port, cfg.Username, cfg.Password, "")); err == nil {
		if _, err = db.Exec("CREATE SCHEMA " + cfg.Name); err == nil {
			if db, err = sql.Open("mysql", dsn(cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Name)); err == nil {
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
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=true&multiStatements=true",
		username, password, host, port, dbName)
}
