// Package ui provides ...
package ui

import (
	"bytes"
	"fmt"
	"os"
	"time"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	"net/http"
)

// Asset loads and returns the asset for the given name. It returns an error if
// the asset could not be found or could not be loaded.
var Asset func(name string) ([]byte, error)

// AssetDir returns the file names below a certain directory in the embedded
// filesystem.
//
// For example, if the embedded filesystem contains the following hierarchy:
//
//     data/
//       foo.txt
//       img/
//         a.png
//         b.png
//
// AssetDir("") returns []string{"data"}
// AssetDir("data") returns []string{"foo.txt", "img"}
// AssetDir("data/img") returns []string{"a.png", "b.png"}
// AssetDir("foo.txt") and AssetDir("notexist") return errors
var AssetDir func(name string) ([]string, error)

// AssetInfo loads and returns metadata for the asset with the given name. It
// returns an error if the asset could not be found or could not be loaded.
var AssetInfo func(name string) (os.FileInfo, error)

// haveUI returns whether the admin UI has been linked into the binary.
func haveUI() bool {
	return Asset != nil && AssetDir != nil && AssetInfo != nil
}

var bareIndexHTML = []byte(fmt.Sprintf(`<!DOCTYPE html>
<title>PD-Web</title>
Binary built without web UI.
<hr>
<em>%s</em>`, "have fun..."))

// Handler returns an http.Handler that serves the UI,
// including index.html, which has some login-related variables
// templated into it, as well as static assets.
func Handler() http.Handler {
	fileServer := http.FileServer(&assetfs.AssetFS{
		Asset:     Asset,
		AssetDir:  AssetDir,
		AssetInfo: AssetInfo,
	})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !haveUI() {
			http.ServeContent(w, r, "index.html", time.Now(), bytes.NewReader(bareIndexHTML))
			return
		}

		fileServer.ServeHTTP(w, r)
	})
}
