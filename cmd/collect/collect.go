package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

func main() {
	// repos := flag.String("R", "", "parent directory containing repos in separate child directories")
	// db := flag.String("d", "", "file path for SQLite database file of the results")
	// n := flag.Int("n", 1, "number of child directories to process in parallel (only works when -R is used)")
	// flag.Parse()
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
}

type RepoList []string

type Obtainer interface {
	Obtain(string) (Results, error)
}

type Persister interface {
	Persist(Results) error
}

func NewCollSrvc(o Obtainer, p Persister) CollSrvc {
	return CollSrvc{o, p}
}

func (cs CollSrvc) CollectLogs(rl RepoList) error {
	for _, r := range rl {
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
	return res, nil
}

//// Access Mercurial process infrastructure

type Proc struct {
	Cmd    exec.Cmd
	Path   string
	StdErr string
	StdOut string
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
			`INSERT INTO logs VALUES %s`,
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
			ev = append(
				ev,
				fmt.Sprintf(
					"('%s','%s','%s')",
					e.TS, e.Err, e.Path,
				),
			)
		}
		eSQL := fmt.Sprintf(
			`INSERT INTO errors VALUES %s`,
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
