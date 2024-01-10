// Copyright 2024 Block, Inc.

package client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync/atomic"
	"time"

	myerr "github.com/go-mysql/errors"

	"github.com/square/finch"
	"github.com/square/finch/data"
	"github.com/square/finch/stats"
	"github.com/square/finch/trx"
)

var (
	ConnectTimeout   = 500 * time.Millisecond
	ConnectRetryWait = 200 * time.Millisecond
)

// Client executes SQL statements. Each client is created in workload.Allocator.Clients
// and run in Stage.Run. Client.Init must be called once before calling Client.Run once.
type Client struct {
	// Required args
	DB         *sql.DB `deep:"-"`
	Data       []StatementData
	DoneChan   chan *Client
	RunLevel   finch.RunLevel
	Statements []*trx.Statement
	Stats      []*stats.Trx `deep:"-"`

	// Optional, usually from stage config
	DefaultDb        string
	IterExecGroup    uint32
	IterExecGroupPtr *uint32
	IterClients      uint32
	IterClientsPtr   *uint32
	Iter             uint
	QPS              <-chan bool
	TPS              <-chan bool

	// Retrun value to DoneChane
	Error Error

	// --
	ps     []*sql.Stmt
	values [][]interface{}
	conn   *sql.Conn
}

type Error struct {
	Err         error
	StatementNo int
}

type StatementData struct {
	Inputs      []data.ValueFunc `deep:"-"` // input to query
	Outputs     []interface{}    `deep:"-"` // output from query; values are data.Generator
	InsertId    data.Generator   `deep:"-"`
	TrxBoundary byte
}

func (c *Client) Init() error {
	c.ps = make([]*sql.Stmt, len(c.Statements))
	c.values = make([][]interface{}, len(c.Statements))
	for i, s := range c.Statements {
		if len(s.Inputs) > 0 {
			c.values[i] = make([]interface{}, len(s.Inputs))
		}
	}
	c.Error = Error{}
	return nil
}

func (c *Client) Connect(ctx context.Context, cerr error, stmtNo int, trxActive bool) error {
	if ctx.Err() != nil { // finch terminated (CTRL-C)?
		return ctx.Err()
	}

	// @todo: handled errors aren't printed, can't tell what went wrong
	//        when errors col != 0

	silent := false
	// Connect called due to error on query execution?
	if cerr != nil {
		errFlags, handled := finch.MySQLErrorHandling[myerr.MySQLErrorCode(cerr)]
		if c.Statements[stmtNo].DDL && !handled {
			return fmt.Errorf("DDL: %s", cerr)
		}
		if handled {
			if errFlags&finch.Eabort != 0 {
				return cerr // stop client
			}
			if errFlags&finch.Erollback != 0 && trxActive {
				finch.Debug("%s: rollback", c.RunLevel.ClientId())
				if _, err := c.conn.ExecContext(ctx, "ROLLBACK"); err != nil {
					return fmt.Errorf("ROLLBACK failed: %s (on err: %s) (query: %s)", err, cerr, c.Statements[stmtNo].Query)
				}
			}
			if errFlags&finch.Econtinue != 0 {
				return nil // keep conn, next iter, keep executing
			}
		}
		silent = (errFlags&finch.Esilent != 0) // log the error (here and below)? uhandled errors are logged
		if !silent {
			log.Printf("Client %s reconnect on error: %s (%s)", c.RunLevel.ClientId(), cerr, c.Statements[stmtNo].Query)
		}
	}

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		time.Sleep(ConnectRetryWait)
	}

	t0 := time.Now()
	for ctx.Err() == nil {
		ctxConn, cancel := context.WithTimeout(ctx, ConnectTimeout)
		c.conn, _ = c.DB.Conn(ctxConn)
		cancel()
		if c.conn != nil {
			break // success
		}
		time.Sleep(ConnectRetryWait)
	}

	if ctx.Err() != nil { // finch terminated (CTRL-C)?
		return ctx.Err()
	}

	if cerr != nil && !silent {
		log.Printf("Client %s reconnected in %.3fs", c.RunLevel.ClientId(), time.Now().Sub(t0).Seconds())
	}

	if c.DefaultDb != "" {
		_, err := c.conn.ExecContext(ctx, "USE `"+c.DefaultDb+"`")
		if err != nil {
			return err
		}
	}

	var err error
	for i, s := range c.Statements {
		if !s.Prepare {
			continue
		}
		if c.ps[i] != nil {
			continue // prepare multi
		}
		c.ps[i], err = c.conn.PrepareContext(ctx, s.Query)
		if err != nil {
			c.Error.StatementNo = i
			return fmt.Errorf("prepare: %s", err)
		}

		// If s.PrepareMulti = 3, it means this ps should be used for 3 statments
		// including this one, so copy it into the next 2 statements. If = 0, this
		// loop doesn't run becuase j = 1; j < 0 is immediately false.
		for j := 1; j < s.PrepareMulti; j++ {
			c.ps[i+j] = c.ps[i]
		}
	}
	return nil
}

func (c *Client) Run(ctxExec context.Context) {
	finch.Debug("run client %s: %d stmts, iter %d/%d/%d", c.RunLevel.ClientId(), len(c.Statements), c.IterExecGroup, c.IterClients, c.Iter)
	var err error
	defer func() {
		if r := recover(); r != nil {
			b := make([]byte, 4096)
			n := runtime.Stack(b, false)
			err = fmt.Errorf("PANIC: %v\n%s", r, string(b[0:n]))
		}
		for i := range c.ps {
			if c.ps[i] == nil {
				continue
			}
			c.ps[i].Close()
		}
		if c.conn != nil {
			c.conn.Close()
		}
		// Context cancellation is not an error it's runtime elapsing or CTRL-C
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			c.Error.Err = err
		}
		c.DoneChan <- c
	}()

	if err = c.Connect(ctxExec, nil, -1, false); err != nil {
		return
	}

	var rc data.RunCount
	rc[data.CONN] = 1 // first MySQL connection ^

	// Not counts but passed with RunCount in case a data.Generator wants to know
	rc[data.CLIENT] = c.RunLevel.Client
	rc[data.CLIENT_GROUP] = c.RunLevel.ClientGroup
	rc[data.EXEC_GROUP] = c.RunLevel.ExecGroup
	rc[data.STAGE] = c.RunLevel.Stage

	var rows *sql.Rows
	var res sql.Result
	var t time.Time

	// trxNo indexes into c.Stats and resets to 0 on each iteration. Remember:
	// these are finch trx (files), not MySQL trx, so trx boundaries mark the
	// beginning and end of a finch trx (file). User is expected to make finch
	// trx boundaries meaningful.
	trxNo := -1
	trxActive := false

	//
	// CRITICAL LOOP: no debug or superfluous function calls
	//
ITER:
	for {
		if c.IterExecGroup > 0 && atomic.AddUint32(c.IterExecGroupPtr, 1) > c.IterExecGroup {
			return
		}
		if c.IterClients > 0 && atomic.AddUint32(c.IterClientsPtr, 1) > c.IterClients {
			return
		}
		if c.Iter > 0 && rc[data.ITER] == c.Iter {
			return
		}
		rc[data.ITER] += 1
		trxNo = -1
		trxActive = false

		for i := range c.Statements {
			// Idle time
			if c.Statements[i].Idle != 0 {
				time.Sleep(c.Statements[i].Idle)
				continue
			}

			// Is this query the start of a new (finch) trx file? This is not
			// a MySQL trx (either BEGIN or implicit). It marks finch trx scope
			// "trx" is a trx file in the config assigned to this client.
			if c.Data[i].TrxBoundary&trx.BEGIN != 0 {
				rc[data.TRX] += 1
				trxNo += 1
				trxActive = true
			} else if c.Data[i].TrxBoundary&trx.END != 0 {
				trxActive = false
			}

			// If BEGIN, check TPS rate limiter
			if c.TPS != nil && c.Statements[i].Begin {
				<-c.TPS
			}

			// If query, check QPS
			if c.QPS != nil {
				<-c.QPS
			}

			// Generate new data values for this query. A single data generator
			// can return multiple values, so d makes copy() append, else copy()
			// would start at [0:] each time
			rc[data.STATEMENT] += 1
			d := 0
			for _, f := range c.Data[i].Inputs {
				d += copy(c.values[i][d:], f(rc))
			}

			if c.Statements[i].ResultSet {
				//
				// SELECT
				//
				t = time.Now()
				if c.ps[i] != nil {
					rows, err = c.ps[i].QueryContext(ctxExec, c.values[i]...)
				} else {
					rows, err = c.conn.QueryContext(ctxExec, fmt.Sprintf(c.Statements[i].Query, c.values[i]...))
				}
				if c.Stats[trxNo] != nil {
					c.Stats[trxNo].Record(stats.READ, time.Now().Sub(t).Microseconds())
				}
				if err != nil {
					goto ERROR
				}
				if c.Data[i].Outputs != nil {
					// @todo what if no row match? This loop won't happen,
					// and the column generator won't be called, which will
					// make it return nil later when used as input to another
					// query.
					for rows.Next() {
						if err = rows.Scan(c.Data[i].Outputs...); err != nil {
							rows.Close()
							goto ERROR
						}
					}
				}
				rows.Close()
			} else {
				//
				// Write or query without result set (e.g. BEGIN, SET, etc.)
				//
				if c.Statements[i].Limit != nil { // limit rows -------------
					if !c.Statements[i].Limit.More(c.conn) {
						return // chan closed = no more writes
					}
				}
				t = time.Now()
				if c.ps[i] != nil { // exec ---------------------------------
					res, err = c.ps[i].ExecContext(ctxExec, c.values[i]...)
				} else {
					res, err = c.conn.ExecContext(ctxExec, fmt.Sprintf(c.Statements[i].Query, c.values[i]...))
				}
				if c.Stats[trxNo] != nil { // record stats ------------------
					switch {
					case c.Statements[i].Write:
						c.Stats[trxNo].Record(stats.WRITE, time.Now().Sub(t).Microseconds())
					case c.Statements[i].Commit:
						c.Stats[trxNo].Record(stats.COMMIT, time.Now().Sub(t).Microseconds())
					default:
						// BEGIN, SET, and other statements that aren't reads or writes
						// but count and response time will be included in total
						c.Stats[trxNo].Record(stats.TOTAL, time.Now().Sub(t).Microseconds())
					}
				}
				if err != nil { // handle err, if any -----------------------
					goto ERROR
				}
				if c.Statements[i].Limit != nil { // limit rows -------------
					n, _ := res.RowsAffected()
					c.Statements[i].Limit.Affected(n)
				}
				if c.Data[i].InsertId != nil { // insert ID -----------------
					id, _ := res.LastInsertId()
					c.Data[i].InsertId.Scan(id)
				}
			} // execute
			continue // next query

		ERROR:
			if c.Stats[trxNo] != nil && ctxExec.Err() == nil {
				c.Stats[trxNo].Error(myerr.MySQLErrorCode(err))
			}
			if err = c.Connect(ctxExec, err, i, trxActive); err != nil {
				c.Error.StatementNo = i
				return // unrecoverable error or runtime elapsed (context timeout/cancel)
			}
			rc[data.CONN] += 1 // reconnected or recovered after query error
			continue ITER
		} // statements
	} // iterations
}
