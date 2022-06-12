package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	var (
		repos  = flag.String("R", "", "parent directory containing repos in separate child directories (if set, will ignore -r)")
		repo   = flag.String("r", "", "a single repo directory (will be ingored if -R is set)")
		dbFile = flag.String("d", "", "file path for SQLite database file of the results")
		n      = flag.Int("n", 1, "parallel workers to process repo directories (only works when -R is used)")
	)

	flag.Parse()

	fmt.Printf("!!! using dbFile: %#v\n", *dbFile)
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

	store := NewStore(db)
	drdr := NewDataReader(NewProc())

	cs := NewCollSrvc(drdr, store)
	cs.WorkerCount = *n

	rl := RepoList{}
	switch {
	case repos != nil:
		fmt.Printf("!!! using repos dir: %#v\n", *repos)
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
	case repo != nil:
		fmt.Printf("!!! repo dir: %#v\n", *repo)

		rl = RepoList{*repo}
	default:
		panic("no repos specified, aborting")
	}

	if err := cs.CollectLogs(rl); err != nil {
		panic(fmt.Sprintf("fatal error collecting logs: %v", err))
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
	WorkerCount int
}

const defWorkers int = 1

type RepoList []string

type Obtainer interface {
	Obtain(string) (Results, error)
}

type Persister interface {
	Persist(Results) error
}

func NewCollSrvc(o Obtainer, p Persister) CollSrvc {
	return CollSrvc{o, p, defWorkers}
}

func (cs CollSrvc) CollectLogs(rl RepoList) error {
	for _, r := range rl {
		fmt.Printf("!!! processing repo: %#v\n", r)
		res, err := cs.Obtain(r)
		if err != nil {
			return err
		}

		if err := cs.Persist(res); err != nil {
			return err
		}
	}
	return nil
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
		e := ErrorEvent{
			TS:   time.Now().String(),
			Err:  err,
			Path: repo,
		}
		res.ErrEvents = append(res.ErrEvents, e)
	}

	ss := strings.Split(str, "\n")
	fmt.Printf("!!! ss: %#v\n", ss)
	for _, rec := range ss {
		if rec != "\"" {
			clean := strings.Trim(rec, "'")
			fmt.Printf("!!! clean: %v\n", clean)
			c := csv.NewReader(strings.NewReader(clean))
			c.Comma = '\t'
			cres, err := c.ReadAll()
			if err != nil {
				e := ErrorEvent{
					TS:   time.Now().String(),
					Err:  err,
					Path: repo,
				}
				res.ErrEvents = append(res.ErrEvents, e)
			}
			if len(cres) != 0 {
				fmt.Printf("!!! cres: %#v\n", cres)
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
				fmt.Printf("!!! r: %#v\n", r)
				res.LogRecs = append(res.LogRecs, r)
			}
		}
	}
	fmt.Printf("!!! Results: %#v\n", res)
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

	fmt.Printf("!!! Total captured string bytes: %v\n", outB.Len())

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

	fmt.Printf("!!! persisting results: %#v\n", res)

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
		fmt.Printf("!!! rSQL: %#v\n", rSQL)
		rStmt, err := tx.Prepare(rSQL)
		if err != nil {
			return err
		}
		if _, err := rStmt.Exec(); err != nil {
			return err
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
		fmt.Printf("!!! eSQL: %#v\n", eSQL)
		eStmt, err := tx.Prepare(eSQL)
		if err != nil {
			return err
		}
		if _, err := eStmt.Exec(); err != nil {
			return err
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
