package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-andiamo/columbus"
	"github.com/lib/pq"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	cfg := config{
		Host:     "localhost",
		Port:     52718,
		Username: "admin",
		Password: "1234",
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
		if pgErr, ok := err.(*pq.Error); ok {
			if pgErr.Code != "42P07" {
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
		"col_f": `{"foo":"bar"}`,
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
	arg := 1
	for k, v := range row {
		cols = append(cols, k)
		args = append(args, v)
		markers = append(markers, fmt.Sprintf("$%d", arg))
		arg++
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
    	col_f JSONB,
    	col_g DECIMAL(20,3),
    	col_h BOOL,
    	col_i INT,
    	col_j FLOAT,
    	col_k BIGINT,
    	col_m REAL,
    	col_n BIT
    )`, name))
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
	db, err := sql.Open("postgres", Dsn(cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Name))
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if pgErr, ok := err.(*pq.Error); ok && pgErr.Code == "3D000" {
		// database (catalog) does not exist - try creating it...
		return createDatabase(cfg)
	}
	return db, err
}

func createDatabase(cfg config) (*sql.DB, error) {
	if db, err := sql.Open("postgres", Dsn(cfg.Host, cfg.Port, cfg.Username, cfg.Password, "postgres")); err == nil {
		if _, err := db.Exec("CREATE DATABASE " + cfg.Name); err == nil {
			_ = db.Close()
			if db, err := sql.Open("postgres", Dsn(cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Name)); err == nil {
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
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		username, password, host, port, dbName)
}
