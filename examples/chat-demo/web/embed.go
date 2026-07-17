// Package web contains the embedded browser client assets.
package web

import "embed"

// Assets contains the browser client served by the demo.
//
//go:embed index.html app.js storage.js styles.css
var Assets embed.FS
