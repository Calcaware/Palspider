package main

import (
	"context"
	"embed"
	"encoding/json"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"github.com/calcaware/palspider/engine"

	"github.com/calcaware/palspider/store"
)

//go:embed views/* public/*
var embeddedFS embed.FS

func main() {
	workerCount := 50
	if n := os.Getenv("INQUEST_WORKERS"); n != "" {
		if v, err := strconv.Atoi(n); err == nil && v > 0 {
			workerCount = v
		}
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	dbPath := os.Getenv("INQUEST_DB")
	if dbPath == "" {
		dbPath = "palspider.db"
	}

	st, err := store.Open(dbPath)
	if err != nil {
		log.Fatalf("Failed to open store: %v", err)
	}
	defer st.Close()

	e := engine.New(workerCount, st)

	if err := e.Restore(); err != nil {
		log.Fatalf("Failed to restore: %v", err)
	}

	if e.Stats().Documents == 0 {
		seed(e)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	tmpl := parseTemplates()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", handleSearch(e, tmpl))
	mux.HandleFunc("GET /search", handleResults(e, tmpl))
	mux.HandleFunc("POST /crawl", handleCrawl(e))
	mux.HandleFunc("GET /crawl-status", handleCrawlStatus(e))
	mux.HandleFunc("GET /suggest", handleSuggest(e))

	mux.HandleFunc("GET /api/status", handleAPIStatus(e))
	mux.HandleFunc("GET /api/peers", handleAPIPeersList(e))
	mux.HandleFunc("POST /api/peers", handleAPIPeersAdd(e))
	mux.HandleFunc("DELETE /api/peers", handleAPIPeersRemove(e))
	mux.HandleFunc("GET /api/search", handleAPISearch(e))
	mux.HandleFunc("GET /api/peers/gossip", handleAPIPeersGossip(e))

	sub, _ := fs.Sub(embeddedFS, "public")
	mux.Handle("GET /style.css", http.FileServer(http.FS(sub)))

	server := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("Shutting down...")
		e.Stop()
		cancel()
		server.Close()
	}()

	log.Printf("Palspider running at http://localhost:%s (%d workers)", port, workerCount)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func seed(e *engine.Engine) {
	docs := []struct{ url, title, text string }{
		{"https://nodejs.org", "Node.js",
			"Node.js is a JavaScript runtime built on V8. It uses an event-driven non-blocking I/O model. Node.js is designed for building scalable network applications. npm is the package manager for Node.js with millions of packages."},
		{"https://expressjs.com", "Express.js",
			"Express is a minimal and flexible Node.js web application framework. It provides a robust set of features for web and mobile applications. Express is the most popular web framework for Node.js."},
		{"https://getbootstrap.com", "Bootstrap",
			"Bootstrap is a powerful front-end framework for building responsive mobile-first sites. It includes HTML CSS and JavaScript components. Bootstrap 5 is the latest version with improved performance."},
		{"https://developer.mozilla.org", "MDN Web Docs",
			"MDN Web Docs is a comprehensive resource for web developers. It covers HTML CSS JavaScript and web APIs. MDN is maintained by Mozilla and the open source community."},
		{"https://github.com", "GitHub",
			"GitHub is a platform for version control and collaboration. It lets you work together on projects using Git. GitHub hosts millions of repositories and is the largest source code host."},
		{"https://en.wikipedia.org/wiki/Search_engine", "Search Engine",
			"A search engine is a software system that finds information on the web. It uses web crawling indexing and ranking to return relevant results. Google Bing and DuckDuckGo are popular search engines."},
		{"https://www.cloudflare.com/learning/dns/what-is-dns/", "DNS",
			"DNS stands for Domain Name System. It translates domain names to IP addresses. DNS is a fundamental protocol that powers the internet by mapping human readable names to numerical addresses."},
	}
	for _, d := range docs {
		e.AddDocument(d.url, d.title, d.text)
	}
	log.Printf("Seeded %d documents", len(docs))
}

func parseTemplates() *template.Template {
	return template.Must(template.ParseFS(embeddedFS, "views/*.html"))
}

func handleSearch(e *engine.Engine, tmpl *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats := e.Stats()
		data := map[string]interface{}{
			"Query": "",
			"Stats": stats,
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		tmpl.ExecuteTemplate(w, "search.html", data)
	}
}

func handleResults(e *engine.Engine, tmpl *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if query == "" {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		results := e.Search(query, 10)
		stats := e.Stats()
		data := map[string]interface{}{
			"Query":   query,
			"Results": results,
			"Stats":   stats,
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		tmpl.ExecuteTemplate(w, "results.html", data)
	}
}

func handleCrawl(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		url := r.FormValue("url")
		if url == "" {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		e.Seed(url)
		http.Redirect(w, r, "/?crawl=started", http.StatusSeeOther)
	}
}

func handleCrawlStatus(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.CrawlStatus())
	}
}

func handleSuggest(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		prefix := r.URL.Query().Get("q")
		if prefix == "" {
			json.NewEncoder(w).Encode([]string{})
			return
		}
		suggestions := e.Suggest(prefix, 5)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(suggestions)
	}
}

func handleAPIStatus(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s := e.Stats()
		status := struct {
			App       string `json:"app"`
			Documents int    `json:"documents"`
			Terms     int    `json:"terms"`
			Visited   int    `json:"visited"`
			Peers     int    `json:"peers"`
		}{
			App:       "palspider",
			Documents: s.Documents,
			Terms:     s.Terms,
			Visited:   s.Visited,
			Peers:     len(e.Peers.List()),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	}
}

func handleAPIPeersList(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Peers.List())
	}
}

func handleAPIPeersAdd(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
			return
		}
		if body.URL == "" {
			http.Error(w, `{"error":"url required"}`, http.StatusBadRequest)
			return
		}
		if err := e.Peers.Add(body.URL); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}
}

func handleAPIPeersRemove(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
			return
		}
		e.Peers.Remove(body.URL)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}
}

func handleAPISearch(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if query == "" {
			http.Error(w, `{"error":"query required"}`, http.StatusBadRequest)
			return
		}
		limit := 10
		if l := r.URL.Query().Get("limit"); l != "" {
			if v, err := strconv.Atoi(l); err == nil && v > 0 {
				limit = v
			}
		}
		results := e.SearchLocal(query, limit)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	}
}

func handleAPIPeersGossip(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Peers.List())
	}
}
