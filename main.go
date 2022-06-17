package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	// handle flags
	var (
		debug  = flag.Bool("D", false, "enable debug logging")
		repos  = flag.String("R", "", "parent directory containing repos in separate child directories (if set, will ignore -r)")
		repo   = flag.String("r", "", "a single repo directory (will be ingored if -R is set)")
		dbFile = flag.String("d", "", "file path for SQLite database file of the results")
		n      = flag.Int("n", 1, "parallel workers to process repo directories (only works when -R is used)")
	)

	flag.Parse()

	// setup global logging
	Log = newAppLog(logConfig{debug: *debug})

	// setup database
	Log.Infof("using dbFile: %#v", *dbFile)
	db, err := sql.Open("sqlite3", *dbFile)
	if err != nil {
		panic(fmt.Sprintf("fatal database open error: %v", err))
	}
	tx, err := db.Begin()
	if err != nil {
		panic(fmt.Sprintf("fatal database txn begin error: %v", err))
	}
	defer tx.Rollback()
	sqlB, err := ioutil.ReadFile("./db-migration.sql")
	if err != nil {
		panic(err)
	}
	if _, err := tx.Exec(string(sqlB)); err != nil {
		panic(err)
	}
	if err := tx.Commit(); err != nil {
		panic(err)
	}
	defer db.Close()

	// setup injected dependencies
	store := NewStore(db)
	drdr := NewDataReader(NewProc())

	// setup workload
	rl := RepoList{}
	var jobs int
	switch {
	case *repos != "":
		jobs = *n
		Log.Infof("using repos dir: %#v", *repos)
		dir, err := os.ReadDir(filepath.Clean(*repos))
		if err != nil {
			panic(fmt.Sprintf("fatal repo directory read error: %v", err))
		}

		for _, r := range dir {
			if r.IsDir() {
				path := filepath.Join(filepath.Clean(*repos), r.Name())
				rl = append(rl, path)
			}
		}
	case *repo != "":
		jobs = 1
		Log.Infof("repo dir: %#v", *repo)

		rl = RepoList{*repo}
	default:
		panic("no repos specified, aborting")
	}

	// begin execution
	start := time.Now()
	cs := NewCollSrvc(drdr, store, jobs)
	Log.Infof("STARTING. Worker pool size: %v", cap(cs.WorkerPool))
	cs.CollectLogs(rl)

	cs.WG.Wait()
	Log.Infof("DONE. Time elapsed: %v", time.Since(start).String())
}

var Log appLog

func (l appLog) Infof(format string, v ...interface{}) {
	l.logger.Printf(format, v...)
}

func (l appLog) Debugf(format string, v ...interface{}) {
	if l.logger.Prefix() == debugPrefix {
		l.logger.Printf(format, v...)
	}
}

type appLog struct {
	logger *log.Logger
}

type logConfig struct {
	debug bool
}

const debugPrefix string = "DEBUG "

func newAppLog(lc logConfig) appLog {
	switch {
	case lc.debug:
		return appLog{
			logger: log.New(
				os.Stdout, debugPrefix, log.LstdFlags|log.Lmsgprefix,
			),
		}
	default:
		return appLog{
			logger: log.New(
				os.Stdout, "", log.LstdFlags|log.Lmsgprefix,
			),
		}
	}
}

type Results struct {
	LogRecs   []LogRecord
	ErrEvents []ErrorEvent
}

type LogRecord struct {
	TS        string
	NodeID    string
	RevID     string
	ParentIDs string
	Author    string
	Tags      string
	Branch    string
	DiffStat  string
	Files     string
	GraphNode string
	RepoPath  string
}

type ErrorEvent struct {
	TS   string
	Err  error
	Path string
}

//// USECASE: LogCollection
//// Q: What do I want to do?
//// A: Take formatted logs from Mercurial and put them in a database.
//// Requirements:
//// 	* process command handler
//// 	* database connector
//// Q: How does "Log Collection" operation take place?
//// A: use a process command handler to query, handle response (both wanted
//// data and unwanted errors), then insert all results into a database.

type CollSrvc struct {
	Obtainer
	Persister
	WorkerPool chan struct{}
	WG         sync.WaitGroup
}

type RepoList []string

type Obtainer interface {
	Obtain(string) (Results, error)
}

type Persister interface {
	Persist(Results) error
}

func NewCollSrvc(o Obtainer, p Persister, n int) *CollSrvc {
	return &CollSrvc{
		Obtainer:   o,
		Persister:  p,
		WorkerPool: make(chan struct{}, n),
	}
}

func (cs *CollSrvc) CollectLogs(rl RepoList) {
	var cnt int
	for _, r := range rl {
		cnt++
		Log.Infof("processing repo %v of %v: %#v", cnt, len(rl), r)
		// Log.Debugf("goroutine count: %v", runtime.NumGoroutine())
		cs.WG.Add(1)
		cs.WorkerPool <- struct{}{}
		Log.Infof("pool worker %v started...", cnt)

		go func(repo string, count int) {
			res, err := cs.Obtain(repo)
			if err != nil {
				Log.Infof("ERROR EVENT LOGGED - %v", err)
				e := ErrorEvent{
					TS:   time.Now().String(),
					Err:  err,
					Path: repo,
				}
				res.ErrEvents = append(res.ErrEvents, e)
			}

			if err := cs.Persist(res); err != nil {
				Log.Infof("ERROR in persisting logs: %v", err)
			}

			<-cs.WorkerPool
			cs.WG.Done()
			Log.Infof("completed repo %v", count)
		}(r, cnt)
	}
}

//// Adapt data from query process

type DataReader struct {
	LogQueryer
}

func NewDataReader(lq LogQueryer) DataReader {
	return DataReader{lq}
}

type LogQueryer interface {
	QueryLogs(string) (string, error)
}

func (dr DataReader) Obtain(repo string) (Results, error) {
	res := Results{LogRecs: []LogRecord{}, ErrEvents: []ErrorEvent{}}
	str, err := dr.QueryLogs(repo)
	if err != nil {
		Log.Infof("ERROR EVENT LOGGED - %v", err)
		e := ErrorEvent{
			TS:   time.Now().String(),
			Err:  err,
			Path: repo,
		}
		res.ErrEvents = append(res.ErrEvents, e)
	}

	ss := strings.Split(str, "\n")
	Log.Debugf("ss: %#v", ss)
	for _, rec := range ss {
		if rec != "\"" {
			clean := strings.Trim(rec, "'")
			Log.Debugf("clean: %v", clean)
			c := csv.NewReader(strings.NewReader(clean))
			c.Comma = '\t'
			cres, err := c.ReadAll()
			if err != nil {
				Log.Infof("ERROR EVENT LOGGED - %v", err)
				e := ErrorEvent{
					TS:   time.Now().String(),
					Err:  err,
					Path: repo,
				}
				res.ErrEvents = append(res.ErrEvents, e)
			}
			if len(cres) != 0 {
				Log.Debugf("cres: %#v", cres)
				r := LogRecord{
					TS:        cres[0][0],
					NodeID:    cres[0][1],
					RevID:     cres[0][2],
					ParentIDs: cres[0][3],
					Author:    cres[0][4],
					Tags:      cres[0][5],
					Branch:    cres[0][6],
					DiffStat:  cres[0][7],
					Files:     cres[0][8],
					GraphNode: cres[0][9],
					RepoPath:  repo,
				}
				Log.Debugf("r: %#v", r)
				res.LogRecs = append(res.LogRecs, r)
			}
		}
	}
	Log.Debugf("Results: %#v", res)
	return res, nil
}

//// Access Mercurial process infrastructure

type Proc struct{}

func NewProc() Proc {
	return Proc{}
}

func (p Proc) QueryLogs(repo string) (string, error) {
	hg, err := exec.LookPath("hg")
	if err != nil {
		panic(err)
	}
	t := `'{date|isodatesec}\t{node}\t{rev}\t{parents}\t{author}\t{tags}\t{branch}\t{diffstat}\t{files}\t{graphnode}\n'`
	args := []string{"log", repo, "--template", t}

	cmd := exec.Command(hg, args...)
	var outB, errB strings.Builder
	cmd.Stdout = &outB
	cmd.Stderr = &errB

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("%w - %v", err, errB.String())
	}
	if err := cmd.Wait(); err != nil {
		return "", fmt.Errorf("%w - %v", err, errB.String())
	}

	Log.Debugf("Total captured string bytes: %v", outB.Len())

	return outB.String(), nil
}

//// Adapt data to persistent storage

type Store struct {
	DB   *sql.DB
	Lock chan struct{}
}

func NewStore(db *sql.DB) *Store {
	return &Store{
		DB:   db,
		Lock: make(chan struct{}, 1),
	}
}

func (st *Store) Persist(res Results) error {
	st.Lock <- struct{}{}
	defer func() {
		<-st.Lock
	}()

	Log.Debugf("persisting results: %#v", res)

	tx, err := st.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var toCommit bool

	if len(res.LogRecs) > 0 {
		var rv []string
		for _, r := range res.LogRecs {
			rv = append(
				rv,
				fmt.Sprintf(
					"('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')",
					r.TS,
					r.NodeID,
					r.RevID,
					r.ParentIDs,
					r.Author,
					r.Tags,
					r.Branch,
					r.DiffStat,
					r.Files,
					r.GraphNode,
					r.RepoPath,
				),
			)
		}
		rSQL := fmt.Sprintf(
			`INSERT INTO logs (ts, node_id, rev_id, parent_ids, author, tags, branch, diffstat, files, graph_node, repo_path)
			VALUES %s`,
			strings.Join(rv, ", "),
		)
		Log.Debugf("rSQL: %#v", rSQL)
		rStmt, err := tx.Prepare(rSQL)
		if err != nil {
			return fmt.Errorf("%w: preparing SQL: %v", err, rSQL)
		}
		if _, err := rStmt.Exec(); err != nil {
			return fmt.Errorf("%w: executing SQL: %v", err, rSQL)
		}

		toCommit = true
	}

	if len(res.ErrEvents) > 0 {
		var ev []string
		for _, e := range res.ErrEvents {
			// normalize for sql strings
			eStr := strings.ReplaceAll(e.Err.Error(), "'", "\"")
			ev = append(
				ev,
				fmt.Sprintf(
					"('%s','%s','%s')",
					e.TS, eStr, e.Path,
				),
			)
		}
		eSQL := fmt.Sprintf(
			`INSERT INTO errs (ts, err, repo_path) VALUES %s`,
			strings.Join(ev, ", "),
		)
		Log.Debugf("eSQL: %#v", eSQL)
		eStmt, err := tx.Prepare(eSQL)
		if err != nil {
			return fmt.Errorf("%w: preparing SQL: %v", err, eSQL)
		}
		if _, err := eStmt.Exec(); err != nil {
			return fmt.Errorf("%w: executing SQL: %v", err, eSQL)
		}

		toCommit = true
	}

	if toCommit {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}
