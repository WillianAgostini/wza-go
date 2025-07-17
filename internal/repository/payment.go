package repository

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
	"wza/internal/entity"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

func Init() {
	connect()
	createTableIfNotExists("paymentsDefault")
	createTableIfNotExists("paymentsFallback")
	log.Println("Database connected successfully")
}

func connect() {
	dbDir := "./data"
	dbPath := filepath.Join(dbDir, "payments.db")

	if err := os.MkdirAll(dbDir, 0755); err != nil {
		log.Panicf("failed to create db directory: %v", err)
	}

	var err error
	db, err = sql.Open(
		"sqlite3",
		fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000", dbPath),
	)
	if err != nil {
		log.Panicf("failed to open db: %v", err)
	}

	if _, err := db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
		log.Panicf("failed to set WAL mode: %v", err)
	}
}

func createTableIfNotExists(table string) {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			correlationid TEXT,
			amount REAL,
			requestedat DATETIME
		)`, table)
	if _, err := db.Exec(query); err != nil {
		log.Panicf("failed to create %s: %v", table, err)
	}
}

func insertPayment(table string, req *entity.PaymentRequest) {
	query := fmt.Sprintf(
		"INSERT INTO %s(correlationid, amount, requestedat) VALUES (?, ?, ?)",
		table,
	)
	_, err := db.Exec(query, req.CorrelationId, req.Amount, req.RequestedAt)
	if err != nil {
		log.Printf("insertPayment(%s) error: %v", table, err)
	}
}

func totalByPeriod(table string, from, to time.Time) (int, float64) {
	query := fmt.Sprintf(`
        SELECT COUNT(*), COALESCE(SUM(amount), 0)
        FROM %s
        WHERE requestedat BETWEEN ? AND ?
    `, table)
	row := db.QueryRow(query, from, to)
	var count int
	var total float64
	if err := row.Scan(&count, &total); err != nil {
		log.Printf("totalByPeriod(%s) error: %v", table, err)
		return 0, 0
	}
	return count, total
}

func InsertDefault(req *entity.PaymentRequest) {
	insertPayment("paymentsDefault", req)
}

func TotalByPeriodDefault(from, to time.Time) (int, float64) {
	return totalByPeriod("paymentsDefault", from, to)
}

func InsertFallback(req *entity.PaymentRequest) {
	insertPayment("paymentsFallback", req)
}

func TotalByPeriodFallback(from, to time.Time) (int, float64) {
	return totalByPeriod("paymentsFallback", from, to)
}

func PurgeAllData() error {
	tables := []string{"paymentsDefault", "paymentsFallback"}
	for _, table := range tables {
		query := fmt.Sprintf("DELETE FROM %s", table)
		if _, err := db.Exec(query); err != nil {
			log.Printf("failed to purge table %s: %v", table, err)
			return err
		}
	}
	log.Println("All payment tables purged successfully")
	return nil
}
