package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	hg, err := exec.LookPath("hg")
	if err != nil {
		panic(err)
	}

	src, err := os.MkdirTemp("", "")
	if err != nil {
		panic(err)
	}
	// defer os.RemoveAll(src)
	fmt.Println("working with temporary src:", src)

	msg := "hello world\n"
	file := filepath.Join(src, "hello.txt")
	if err := ioutil.WriteFile(file, []byte(msg), 0644); err != nil {
		panic(err)
	}

	init := exec.Command(hg, "init", src)
	var initErr bytes.Buffer
	init.Stderr = &initErr
	if err := init.Run(); err != nil {
		panic(fmt.Sprintf("%v: %v", err, initErr.String()))
	}

	commit := exec.Command(hg, "commit", "--cwd", src, "-Am", "'initial commit'")
	var commitErr bytes.Buffer
	if err := commit.Run(); err != nil {
		panic(fmt.Sprintf("%v: %v", err, commitErr.String()))
	}

	copies := filepath.Join(os.TempDir(), "data-gen-copies")
	_, err = os.Stat(copies)
	switch {
	case err == nil: // already exists, remove to start fresh
		if err := os.RemoveAll(copies); err != nil {
			panic(err)
		}
	case !errors.Is(err, os.ErrNotExist): // unexpected error
		panic(err)
	case errors.Is(err, os.ErrNotExist): // continue
	}
	if err := os.MkdirAll(copies, 0755); err != nil {
		panic(err)
	}

	for i := 0; i < 1000; i++ {
		dst := filepath.Join(copies, fmt.Sprintf("hg-repo-%03d", i))
		if err := os.MkdirAll(dst, 0755); err != nil {
			panic(err)
		}
		if err := copyDir(dst, src); err != nil {
			panic(err)
		}
	}

	fmt.Println("data generated here:", copies)
}

func copyDir(dst, src string) error {
	dir, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(dst, 0755); err != nil {
		return err
	}

	for _, path := range dir {
		dstPath := filepath.Join(dst, path.Name())
		srcPath := filepath.Join(src, path.Name())
		srcInfo, err := os.Stat(srcPath)
		if err != nil {
			return err
		}

		switch {
		case srcInfo.IsDir():
			if err := os.MkdirAll(dstPath, srcInfo.Mode()); err != nil {
				return err
			}
			if err := copyDir(dstPath, srcPath); err != nil {
				return err
			}
		default:
			df, err := os.OpenFile(
				dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, srcInfo.Mode(),
			)
			if err != nil {
				return err
			}
			defer df.Close()

			sf, err := os.Open(srcPath)
			if err != nil {
				return err
			}
			defer sf.Close()

			if _, err := io.Copy(df, sf); err != nil {
				return err
			}
		}
	}

	return nil
}
