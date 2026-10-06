package peer

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/calcaware/palspider/searcher"
)

type Peer struct {
	URL      string `json:"url"`
	LastSeen string `json:"lastSeen,omitempty"`
	Status   string `json:"status"`
}

type PeerSearchResult struct {
	Peer    string
	Results []searcher.Result
	Error   error
}

type PeerManager struct {
	mu        sync.RWMutex
	peers     map[string]*Peer
	gossiping map[string]bool
	client    *http.Client
}

func New() *PeerManager {
	return &PeerManager{
		peers:     make(map[string]*Peer),
		gossiping: make(map[string]bool),
		client:    &http.Client{Timeout: 10 * time.Second},
	}
}

// normalizePeerURL reduces a peer address to scheme://host[:port] so that
// adds and removes of equivalent URLs match. Only http and https peers are
// accepted.
func normalizePeerURL(peerURL string) (string, error) {
	u, err := url.Parse(peerURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("peer URL must use http or https")
	}
	normalized := u.Scheme + "://" + u.Host
	return normalized, nil
}

func (pm *PeerManager) Add(peerURL string) error {
	normalized, err := normalizePeerURL(peerURL)
	if err != nil {
		return err
	}

	pm.mu.RLock()
	_, exists := pm.peers[normalized]
	pm.mu.RUnlock()
	if exists {
		return nil
	}

	if err := pm.validate(normalized); err != nil {
		return fmt.Errorf("peer validation failed: %w", err)
	}

	pm.mu.Lock()
	pm.peers[normalized] = &Peer{
		URL:      normalized,
		Status:   "connected",
		LastSeen: time.Now().Format(time.RFC3339),
	}
	pm.mu.Unlock()

	pm.startGossip(normalized)

	return nil
}

func (pm *PeerManager) validate(peerURL string) error {
	resp, err := pm.client.Get(peerURL + "/api/status")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}

	var s struct {
		App string `json:"app"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return err
	}
	if s.App != "palspider" {
		return fmt.Errorf("not a Palspider peer")
	}
	return nil
}

// Remove drops a peer, matching the same normalization used by Add. It
// reports whether a peer was actually removed.
func (pm *PeerManager) Remove(peerURL string) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if normalized, err := normalizePeerURL(peerURL); err == nil {
		if _, ok := pm.peers[normalized]; ok {
			delete(pm.peers, normalized)
			return true
		}
	}
	if _, ok := pm.peers[peerURL]; ok {
		delete(pm.peers, peerURL)
		return true
	}
	return false
}

func (pm *PeerManager) List() []*Peer {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	list := make([]*Peer, 0, len(pm.peers))
	for _, p := range pm.peers {
		list = append(list, &Peer{
			URL:      p.URL,
			Status:   p.Status,
			LastSeen: p.LastSeen,
		})
	}
	return list
}

// markContact records a successful or failed exchange with a peer.
func (pm *PeerManager) markContact(peerURL string, err error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	p, ok := pm.peers[peerURL]
	if !ok {
		return
	}
	p.LastSeen = time.Now().Format(time.RFC3339)
	if err != nil {
		p.Status = "unreachable"
	} else {
		p.Status = "connected"
	}
}

// startGossip launches a one-shot gossip exchange, skipping peers that
// already have one in flight so a large mesh does not spawn duplicate
// goroutines for the same target.
func (pm *PeerManager) startGossip(peerURL string) {
	pm.mu.Lock()
	if pm.gossiping[peerURL] {
		pm.mu.Unlock()
		return
	}
	pm.gossiping[peerURL] = true
	pm.mu.Unlock()

	go func() {
		defer func() {
			pm.mu.Lock()
			delete(pm.gossiping, peerURL)
			pm.mu.Unlock()
		}()
		pm.gossipFrom(peerURL)
	}()
}

func (pm *PeerManager) gossipFrom(peerURL string) {
	resp, err := pm.client.Get(peerURL + "/api/peers/gossip")
	if err != nil {
		pm.markContact(peerURL, err)
		return
	}
	defer resp.Body.Close()

	var remotePeers []*Peer
	if err := json.NewDecoder(resp.Body).Decode(&remotePeers); err != nil {
		pm.markContact(peerURL, err)
		return
	}
	pm.markContact(peerURL, nil)

	for _, rp := range remotePeers {
		if err := pm.Add(rp.URL); err != nil {
			log.Printf("peer: gossip add %s: %v", rp.URL, err)
		}
	}
}

func (pm *PeerManager) SearchAll(query string, limit int, timeout time.Duration) []PeerSearchResult {
	pm.mu.RLock()
	peers := make([]*Peer, 0, len(pm.peers))
	for _, p := range pm.peers {
		peers = append(peers, p)
	}
	pm.mu.RUnlock()

	if len(peers) == 0 {
		return nil
	}

	results := make([]PeerSearchResult, len(peers))
	var wg sync.WaitGroup

	for i, p := range peers {
		wg.Add(1)
		go func(i int, p *Peer) {
			defer wg.Done()

			client := &http.Client{Timeout: timeout}
			reqURL := fmt.Sprintf("%s/api/search?q=%s&limit=%d", p.URL, url.QueryEscape(query), limit)
			resp, err := client.Get(reqURL)
			if err != nil {
				pm.markContact(p.URL, err)
				results[i] = PeerSearchResult{Peer: p.URL, Error: err}
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				err = fmt.Errorf("status %d", resp.StatusCode)
				pm.markContact(p.URL, err)
				results[i] = PeerSearchResult{Peer: p.URL, Error: err}
				return
			}

			var peerResults []searcher.Result
			if err := json.NewDecoder(resp.Body).Decode(&peerResults); err != nil {
				pm.markContact(p.URL, err)
				results[i] = PeerSearchResult{Peer: p.URL, Error: err}
				return
			}
			pm.markContact(p.URL, nil)

			for j := range peerResults {
				peerResults[j].PeerSource = p.URL
			}

			results[i] = PeerSearchResult{Peer: p.URL, Results: peerResults}
		}(i, p)
	}

	wg.Wait()
	return results
}
