package status

import (
	"embed"
	"errors"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
)

//go:embed webmanager/dist
var webManagerAssets embed.FS

type webManagerAsset struct {
	path        string
	contentType string
	cache       string
}

func serveWebManager(writer http.ResponseWriter, request *http.Request) bool {
	asset, exists := webManagerAssetForPath(request.URL.Path)
	if !exists {
		return false
	}
	content, err := webManagerAssets.ReadFile(asset.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false
		}
		writeFixedError(writer, request.Method, http.StatusServiceUnavailable, "asset_unavailable")
		return true
	}
	writer.Header().Set("Content-Type", asset.contentType)
	writer.Header().Set("Cache-Control", asset.cache)
	writer.Header().Set("Content-Security-Policy", "default-src 'self'; connect-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
	writer.Header().Set("Referrer-Policy", "no-referrer")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("X-Frame-Options", "DENY")
	writer.Header().Set("Content-Length", strconv.Itoa(len(content)))
	writer.WriteHeader(http.StatusOK)
	if request.Method != http.MethodHead {
		_, _ = writer.Write(content)
	}
	return true
}

func webManagerAssetForPath(requestPath string) (webManagerAsset, bool) {
	if requestPath == "/" || requestPath == "/index.html" {
		return webManagerAsset{
			path:        "webmanager/dist/index.html",
			contentType: "text/html; charset=utf-8",
			cache:       "no-store",
		}, true
	}
	if !strings.HasPrefix(requestPath, "/assets/") || path.Clean(requestPath) != requestPath {
		return webManagerAsset{}, false
	}
	contentType := mime.TypeByExtension(path.Ext(requestPath))
	if contentType == "" {
		return webManagerAsset{}, false
	}
	return webManagerAsset{
		path:        "webmanager/dist" + requestPath,
		contentType: contentType,
		cache:       "public, max-age=31536000, immutable",
	}, true
}
