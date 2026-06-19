package peer

import (
	"encoding/json"
	"fmt"
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
	mu     sync.RWMutex
	peers  map[string]*Peer
	client *http.Client
}

func New() *PeerManager {
	return &PeerManager{
		peers:  make(map[string]*Peer),
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (pm *PeerManager) Add(peerURL string) error {
	u, err := url.Parse(peerURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("invalid URL")
	}

	normalized := u.Scheme + "://" + u.Host
	if u.Port() != "" {
		normalized += ":" + u.Port()
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
		URL:    normalized,
		Status: "connected",
	}
	pm.mu.Unlock()

	go pm.gossipFrom(normalized)

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

func (pm *PeerManager) Remove(peerURL string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.peers, peerURL)
}

func (pm *PeerManager) List() []*Peer {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	list := make([]*Peer, 0, len(pm.peers))
	for _, p := range pm.peers {
		list = append(list, p)
	}
	return list
}

func (pm *PeerManager) gossipFrom(peerURL string) {
	resp, err := pm.client.Get(peerURL + "/api/peers/gossip")
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var remotePeers []*Peer
	if err := json.NewDecoder(resp.Body).Decode(&remotePeers); err != nil {
		return
	}

	for _, rp := range remotePeers {
		pm.Add(rp.URL)
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
				results[i] = PeerSearchResult{Peer: p.URL, Error: err}
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				results[i] = PeerSearchResult{Peer: p.URL, Error: fmt.Errorf("status %d", resp.StatusCode)}
				return
			}

			var peerResults []searcher.Result
			if err := json.NewDecoder(resp.Body).Decode(&peerResults); err != nil {
				results[i] = PeerSearchResult{Peer: p.URL, Error: err}
				return
			}

			for j := range peerResults {
				peerResults[j].PeerSource = p.URL
			}

			results[i] = PeerSearchResult{Peer: p.URL, Results: peerResults}
		}(i, p)
	}

	wg.Wait()
	return results
}
