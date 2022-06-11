package main

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func assert(t *testing.T, got, want interface{}) {
	t.Helper()
	gErr, gErrOk := got.(error)
	wErr, wErrOk := want.(error)
	switch {
	case gErrOk, wErrOk:
		if !errors.Is(gErr, wErr) {
			t.Errorf("got error %v, want error %v", gErr, wErr)
		}
	default:
		if got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func assertDeep(t *testing.T, got, want interface{}) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got:\n%#v\nwant:\n%#v", got, want)
	}
}

const (
	testRepoLog    string = "'2022-06-10 23:43:47 +0000\t71efee2949bd457bac92e3f21215a1bc310fd62f\t0\t\tSome User <some.user@email.com>\ttip\tdefault\t1: +1/-0\thi.txt\t@\n'"
	testRepoLogDbl string = "'2022-06-10 23:43:47 +0000\t71efee2949bd457bac92e3f21215a1bc310fd62f\t0\t\tSome User <some.user@email.com>\ttip\tdefault\t1: +1/-0\thi.txt\t@\n''2022-06-13 03:33:33 +0000\t71efee2949bd457bac92e3f21215a1bc310fd62f\t0\t\tSome User <some.user@email.com>\ttip\tdefault\t1: +1/-0\thi.txt\t@\n'"
)

var (
	testRepo      string = filepath.Clean("../../testdata/test_repo")
	testLogRecord        = LogRecord{
		TS:        "2022-06-10 23:43:47 +0000",
		NodeID:    "71efee2949bd457bac92e3f21215a1bc310fd62f",
		RevID:     "0",
		Author:    "Some User <some.user@email.com>",
		Tags:      "tip",
		Branch:    "default",
		DiffStat:  "1: +1/-0",
		Files:     "hi.txt",
		GraphNode: "@",
		RepoPath:  testRepo,
	}
)

func makeSrvcMocks() (mockObt, mockPer) {
	return mockObt{}, mockPer{}
}

type mockObt struct{}
type mockPer struct{}

func (m mockObt) Obtain(r string) (Results, error) {
	return Results{}, nil
}

func (m mockPer) Persist(res Results) error {
	return nil
}

type mockLogQry struct{}

func (m mockLogQry) QueryLogs(repo string) (string, error) {
	return testRepoLog, nil
}

func TestNewCollSrvc(t *testing.T) {
	t.Run("can init new collection service", func(t *testing.T) {
		o, p := makeSrvcMocks()

		// SUT
		got := NewCollSrvc(o, p)

		if got.Obtainer == nil {
			t.Errorf("got nil, want new collection service obtainer")
		}
		if got.Persister == nil {
			t.Errorf("got nil, want new collection service persister")
		}
	})
}

func TestCollectLogs(t *testing.T) {
	t.Run("can collect logs", func(t *testing.T) {
		o, p := makeSrvcMocks()
		cs := NewCollSrvc(o, p)
		rl := RepoList{testRepoLog}

		// SUT
		err := cs.CollectLogs(rl)

		assert(t, err, nil)
	})
}

func TestNewDataReader(t *testing.T) {
	t.Run("can init new collection service", func(t *testing.T) {
		lq := mockLogQry{}

		// SUT
		got := NewDataReader(lq)

		if got.LogQueryer == nil {
			t.Errorf("got nil, want new collection service persister")
		}
	})
}

func TestObtain(t *testing.T) {
	t.Run("can obtain logs", func(t *testing.T) {
		mq := mockLogQry{}
		dr := DataReader{mq}
		want := Results{
			LogRecs: []LogRecord{testLogRecord},
		}

		// SUT
		got, err := dr.Obtain(testRepo)

		assert(t, err, nil)
		assert(t, len(got.LogRecs), 1)
		gr := got.LogRecs[0]
		wr := want.LogRecs[0]
		assert(t, gr.TS, wr.TS)
		assert(t, gr.NodeID, wr.NodeID)
		assert(t, gr.RevID, wr.RevID)
		assert(t, gr.ParentIDs, wr.ParentIDs)
		assert(t, gr.Author, wr.Author)
		assert(t, gr.Tags, wr.Tags)
		assert(t, gr.Branch, wr.Branch)
		assert(t, gr.DiffStat, wr.DiffStat)
		assert(t, gr.Files, wr.Files)
		assert(t, gr.GraphNode, wr.GraphNode)
		assert(t, gr.RepoPath, wr.RepoPath)
	})
}

func TestQueryLogs(t *testing.T) {
	t.Run("can query hg command for logs", func(t *testing.T) {
		// repo := filepath.Clean("../../testdata/test_repo2")
		repo := testRepo
		proc := Proc{}
		want := testRepoLog

		// SUT
		got, err := proc.QueryLogs(repo)

		assert(t, err, nil)
		// assert(t, got, want)
		if got != want {
			t.Errorf("got\n%#v\nwant\n%#v", got, want)
		}
	})
}

func TestNewStore(t *testing.T) {
	t.Run("can init new collection service", func(t *testing.T) {
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatalf("unexepcted setup error: %v", err)
		}
		defer db.Close()

		// SUT
		got := NewStore(db)

		if got.Lock == nil {
			t.Errorf("got nil, want new store lock")
		}
	})
}

func TestPersist(t *testing.T) {
	t.Run("can persist logs", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("unexepcted setup error: %v", err)
		}
		defer db.Close()

		st := NewStore(db)
		res := Results{
			LogRecs: []LogRecord{testLogRecord},
		}

		var expectCommit bool
		mock.ExpectBegin()
		if len(res.LogRecs) > 0 {
			expectCommit = true
			mock.ExpectPrepare(`INSERT INTO logs`)
			mock.ExpectExec(`INSERT INTO logs`).
				WillReturnResult(sqlmock.NewResult(1, 1))
		}
		if len(res.ErrEvents) > 0 {
			expectCommit = true
			mock.ExpectPrepare(`INSERT INTO errors`)
			mock.ExpectExec(`INSERT INTO errors`).
				WillReturnResult(sqlmock.NewResult(1, 1))
		}
		if expectCommit {
			mock.ExpectCommit()
		}

		// SUT
		err = st.Persist(res)

		assert(t, err, nil)
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("not all sqlmock expecations were met: %v", err)
		}
	})
}
