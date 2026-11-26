// Package web embeds the frontend files served by the standalone Linux server.
package web

import "embed"

// Files holds the static frontend.
//
//go:embed index.html app.js i18n.js style.css
var Files embed.FS
