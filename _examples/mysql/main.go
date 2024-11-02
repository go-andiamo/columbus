package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-andiamo/columbus"
	"github.com/go-sql-driver/mysql"
	"strings"
	"time"
)

func main() {
	cfg := config{
		Host:     "localhost",
		Port:     55000,
		Username: "root",
		Password: "root",
		Name:     "test_db",
	}
	db, err := openDatabase(cfg)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	err = createTestTable(db, "foo")
	if err != nil {
		if myerr, ok := err.(*mysql.MySQLError); ok {
			if myerr.Number != 1050 {
				panic(err)
			}
		} else {
			panic(err)
		}
	}

	err = insert(db, "foo", map[string]any{
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
	if err != nil {
		panic(err)
	}

	const cols = `*`
	mapper, err := columbus.NewMapper(cols, nil, columbus.Query(`FROM foo`))
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	row, err := mapper.FirstRow(ctx, db, nil)
	if err != nil {
		panic(err)
	}
	_ = row
}

func insert(db *sql.DB, tableName string, row map[string]any) error {
	cols := make([]string, 0, len(row))
	args := make([]any, 0, len(row))
	markers := make([]string, 0, len(row))
	for k, v := range row {
		cols = append(cols, k)
		args = append(args, v)
		markers = append(markers, "?")
	}
	query := `INSERT INTO ` + tableName + ` (` + strings.Join(cols, ",") + `) VALUES (` + strings.Join(markers, ",") + `)`
	_, err := db.ExecContext(context.Background(), query, args...)
	return err
}

func createTestTable(db *sql.DB, name string) error {
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (
    	col_a CHAR(20),
    	col_b VARCHAR(20),
    	col_c TIMESTAMP(3)  DEFAULT CURRENT_TIMESTAMP(3),
    	col_d TEXT,
    	col_e JSON,
    	-- col_f JSONB,
    	col_g DECIMAL(20,3),
    	col_h BOOL,
    	col_i INT,
    	col_j FLOAT,
    	col_k BIGINT,
    	col_l TINYINT,
    	col_m REAL,
    	col_n BIT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, name))
	return err
}

type config struct {
	Host     string
	Port     int
	Username string
	Password string
	Name     string
}

func openDatabase(cfg config) (*sql.DB, error) {
	db, err := sql.Open("mysql", Dsn(cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Name))
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if myErr, ok := err.(*mysql.MySQLError); ok && myErr.Number == 1049 {
		// database (catalog) does not exist - try creating it...
		return createDatabase(cfg)
	}
	return db, err
}

func createDatabase(cfg config) (*sql.DB, error) {
	if db, err := sql.Open("mysql", Dsn(cfg.Host, cfg.Port, cfg.Username, cfg.Password, "")); err == nil {
		if _, err := db.Exec("CREATE SCHEMA " + cfg.Name); err == nil {
			if db, err := sql.Open("mysql", Dsn(cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Name)); err == nil {
				return db, db.Ping()
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func Dsn(host string, port int, username, password, dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=true&multiStatements=true",
		username, password, host, port, dbName)
}
