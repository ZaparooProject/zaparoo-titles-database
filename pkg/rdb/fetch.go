package rdb

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func FetchRDB(rdbName string) {
	rdbDir := "./assets/rdb"
	rdbPath := filepath.Join(rdbDir, rdbName)
	if _, err := os.Stat(rdbPath); errors.Is(err, os.ErrNotExist) {
		fmt.Printf("Error openingT %s\n", rdbPath)
		// else fetch remote rdb file, save to filename in cores/{core}/{name}
		fmtFile := strings.Replace(url.QueryEscape(rdbName), "+", "%20", -1)
		url := fmt.Sprintf("%s%s", RootRdbUrl, fmtFile)
		fmt.Printf("Trying GET %s\n", url)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Printf("Unable to GET url %s, skipping core\n", url)
		}
		fmt.Printf("GET StatusCode %v\n", resp.StatusCode)

		fmt.Printf("Trying ReadAll from GET %s\n", rdbName)
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("Unable to Read RDB respone from GET url %s, skipping core\n", url)
		}

		fmt.Printf("Saving RDB Bytes from GET %s\n", rdbName)
		fo, err := os.Create(rdbPath)
		if err != nil {
			fmt.Printf("Unable to write file %s\n", rdbPath)
			return
		}
		fo.Write(body)
		if err := fo.Close(); err != nil {
			panic(err)
		}
		fmt.Println("Saved RDB", rdbName)
	} else {
		fmt.Println("Existing RDB, Skipping", rdbName)
	}
}
