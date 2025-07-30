package limit

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/square/finch"
)

type Data interface {
	Affected(n int64)
	More(*sql.Conn) bool
}

// --------------------------------------------------------------------------

type or struct {
	c chan bool
	n uint64
	a Data
	b Data
}

var _ Data = or{}

// Or makes a Data limiter that allows more data until a or b reaches its limit.
// This is used to combine row and size limits, like "insert 1M rows or 2G of data,
// whichever occurs first."
func Or(a, b Data) Data {
	if a == nil && b == nil {
		return nil
	}
	if a == nil && b != nil {
		return b
	}
	if a != nil && b == nil {
		return a
	}
	lm := or{
		a: a,
		b: b,
		c: make(chan bool, 1),
	}
	return lm
}

func (lm or) Affected(n int64) {
	lm.a.Affected(n)
	lm.b.Affected(n)
}

func (lm or) More(conn *sql.Conn) bool {
	return lm.a.More(conn) && lm.b.More(conn)
}

// --------------------------------------------------------------------------

type Rows struct {
	max int64
	n   int64
	p   float64 // = size / max * 100
	r   uint    // report p every r%
	t   time.Time
	pn  int64
	*sync.Mutex
}

var _ Data = &Rows{}

func NewRows(max, offset int64) *Rows {
	if max == 0 {
		return nil
	}
	lm := &Rows{
		max:   max,
		Mutex: &sync.Mutex{},
		r:     5,
		n:     offset,
		pn:    offset,
	}
	return lm
}

func (lm *Rows) Affected(n int64) {
	lm.Lock()
	lm.n += n
	// Report progress every r%
	p := float64(lm.n) / float64(lm.max) * 100
	if p-lm.p > float64(lm.r) {
		d := time.Now().Sub(lm.t)
		rate := float64(lm.n-lm.pn) / d.Seconds()
		eta := time.Duration(float64(lm.max-lm.n)/rate) * time.Second
		log.Printf("%s / %s = %.1f%% in %s: %s rows/s (ETA %s)\n",
			humanize.Comma(lm.n), humanize.Comma(lm.max), p, d.Round(time.Second), humanize.Comma(int64(rate)), eta)
		lm.p = p
		lm.t = time.Now()
		lm.pn = lm.n
	}
	lm.Unlock()
}

func (lm *Rows) More(_ *sql.Conn) bool {
	lm.Lock()
	if lm.t.IsZero() {
		lm.t = time.Now()
	}
	more := lm.n < lm.max
	lm.Unlock()
	return more
}

// --------------------------------------------------------------------------

type SizeFunc func(*sql.Conn) (uint64, error)

type Size struct {
	max     uint64 // 200000000, converted from maxStr
	maxStr  string // 200MB, exactly as specified by user
	db      string // database-size: DB maxStr
	tbl     string // table-size: TABLE maxStr
	query   string
	analyze string
	n       uint    // calls to More
	m       uint    // how often to check stats: n % m
	p       float64 // = size / max * 100
	r       uint    // report p every r%
	t       time.Time
	bytes   uint64
	*sync.Mutex
}

var _ Data = &Size{}

func NewSize(max uint64, maxStr string, db, tbl string) *Size {
	if db == "" && tbl == "" {
		panic("limit.NewSize called without a db or tbl name")
	}

	// ANALYZE TABLE every n % m == 0. Default m=5 so we don't check too often.
	// But if max size is small, <=1G, that will probably be written very quickly,
	// so check every 3rd call to avoid surpassing the max by too much.
	var m uint = 5
	var r uint = 5
	if max <= 1073741824 { // 1G
		m = 3
		r = 10
	}
	if max >= 107374182400 { // 100 GB
		m = 1000
		r = 2
	}

	finch.Debug("limit size db %s tbl %s = %d bytes (m=%d r=%d)", db, tbl, max, m, r)
	lm := &Size{
		db:     db,
		tbl:    tbl,
		max:    max,
		maxStr: maxStr,
		Mutex:  &sync.Mutex{},
		m:      m,
		r:      r,
	}
	return lm
}

func (lm *Size) Affected(n int64) {
}

func (lm *Size) More(conn *sql.Conn) bool {
	lm.Lock()
	defer lm.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Set queries on first call
	if lm.query == "" {
		lm.query = "SELECT COALESCE(data_length + index_length, 0) AS bytes FROM information_schema.TABLES WHERE "
		if lm.db != "" {
			log.Printf("Database size limit: %s %s (progress report every %d%%)", lm.db, lm.maxStr, lm.r)
			lm.query += "table_schema='" + lm.db + "'"

			var tbls []string
			rows, err := conn.QueryContext(ctx, "SHOW FULL TABLES")
			if err != nil {
				log.Printf("Error running SHOW FULL TABLES: %s", err)
				return false
			}
			defer rows.Close()
			for rows.Next() {
				var name, base string
				err = rows.Scan(&name, &base)
				if err != nil {
					break
				}
				if base != "BASE TABLE" {
					continue
				}
				tbls = append(tbls, name)
			}
			lm.analyze = "ANALYZE TABLE " + strings.Join(tbls, ", ")
		} else {
			log.Printf("Table size limit: %s %s (progress report every %d%%)", lm.tbl, lm.maxStr, lm.r)
			err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&lm.db)
			if err != nil {
				log.Printf("Error getting current database: %s", err)
				return false
			}
			lm.query += "table_schema='" + lm.db + "' AND table_name='" + lm.tbl + "'"
			lm.analyze = "ANALYZE TABLE " + lm.tbl
		}
		finch.Debug(lm.query)
		finch.Debug(lm.analyze)

		lm.t = time.Now()
	}

	// Every few calls, run ANALYZE TABLE to update the stats, then fech latest size
	lm.n++
	if lm.n%lm.m != 0 {
		return true // not time to check; presume there's more to load
	}

	if _, err := conn.ExecContext(ctx, lm.analyze); err != nil {
		log.Printf("Error running ANALYZE TABLE: %s", err)
		return false
	}

	// Get database/table size in bytes
	var bytes uint64
	err := conn.QueryRowContext(ctx, lm.query).Scan(&bytes)
	if err != nil {
		log.Printf("Error query data size: %s", err)
		return false
	}

	// Report progress every r%
	p := float64(bytes) / float64(lm.max) * 100
	if p-lm.p > float64(lm.r) {
		d := time.Now().Sub(lm.t)
		rate := float64(bytes-lm.bytes) / d.Seconds()
		eta := time.Duration(float64(lm.max-bytes)/rate) * time.Second
		log.Printf("%s / %s = %.1f%% in %s: %s/s (ETA %s)\n",
			humanize.Bytes(bytes), lm.maxStr, p, d.Round(time.Second), humanize.Bytes(uint64(rate)), eta)
		lm.p = p
		lm.t = time.Now()
		lm.bytes = bytes
	}

	return bytes < lm.max
}
