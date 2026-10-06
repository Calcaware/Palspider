package peer

import "testing"

func TestNormalizePeerURL(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "http://192.168.1.5:3000", want: "http://192.168.1.5:3000"},
		{in: "http://192.168.1.5:3000/", want: "http://192.168.1.5:3000"},
		{in: "http://192.168.1.5:3000/api/status", want: "http://192.168.1.5:3000"},
		{in: "https://peer.example.com", want: "https://peer.example.com"},
		{in: "ftp://peer.example.com", wantErr: true},
		{in: "not a url", wantErr: true},
		{in: "192.168.1.5:3000", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, c := range cases {
		got, err := normalizePeerURL(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("normalizePeerURL(%q) = %q, want error", c.in, got)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("normalizePeerURL(%q) = %q, %v; want %q", c.in, got, err, c.want)
		}
	}
}

func TestListReturnsCopies(t *testing.T) {
	pm := New()
	pm.mu.Lock()
	pm.peers["http://a:1"] = &Peer{URL: "http://a:1", Status: "connected"}
	pm.mu.Unlock()

	list := pm.List()
	if len(list) != 1 {
		t.Fatalf("List = %d peers, want 1", len(list))
	}
	list[0].Status = "mutated"
	if pm.List()[0].Status != "connected" {
		t.Error("List exposed internal peer state; callers could race with markContact")
	}
}

func TestRemoveReportsWhetherPeerExisted(t *testing.T) {
	pm := New()
	pm.mu.Lock()
	pm.peers["http://a:1"] = &Peer{URL: "http://a:1", Status: "connected"}
	pm.mu.Unlock()

	if !pm.Remove("http://a:1/") {
		t.Error("Remove(http://a:1/) = false, want true (normalization must match Add)")
	}
	if pm.Remove("http://a:1") {
		t.Error("second Remove = true, want false")
	}
	if pm.Remove("http://never-added") {
		t.Error("Remove of unknown peer = true, want false")
	}
}
