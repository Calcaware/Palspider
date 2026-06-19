package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

type Store struct {
	db *bolt.DB
}

func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("bolt open: %w", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"docs", "index", "links", "visited", "fingerprints", "meta"} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) SaveDocument(id int, doc interface{}) error {
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("docs")).Put(itob(id), data)
	})
}

func (s *Store) AllDocuments(fn func(id int, data []byte) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("docs")).ForEach(func(k, v []byte) error {
			return fn(btoi(k), v)
		})
	})
}

func (s *Store) SavePostings(term string, postings interface{}) error {
	data, err := json.Marshal(postings)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("index")).Put([]byte(term), data)
	})
}

func (s *Store) AllPostings(fn func(term string, data []byte) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("index")).ForEach(func(k, v []byte) error {
			return fn(string(k), v)
		})
	})
}

func (s *Store) SaveLinkCount(url string, count int) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("links")).Put([]byte(url), itob(count))
	})
}

func (s *Store) AllLinkCounts(fn func(url string, count int) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("links")).ForEach(func(k, v []byte) error {
			return fn(string(k), btoi(v))
		})
	})
}

func (s *Store) MarkVisited(url string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("visited")).Put([]byte(url), []byte{1})
	})
}

func (s *Store) AllVisited(fn func(url string) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("visited")).ForEach(func(k, _ []byte) error {
			return fn(string(k))
		})
	})
}

func (s *Store) MarkFingerprint(hash string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("fingerprints")).Put([]byte(hash), []byte{1})
	})
}

func (s *Store) AllFingerprints(fn func(hash string) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("fingerprints")).ForEach(func(k, _ []byte) error {
			return fn(string(k))
		})
	})
}

func (s *Store) SaveNextDocID(id int) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("meta")).Put([]byte("nextDocID"), itob(id))
	})
}

func (s *Store) LoadNextDocID() (int, error) {
	var id int
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("meta")).Get([]byte("nextDocID"))
		if v == nil {
			return nil
		}
		id = btoi(v)
		return nil
	})
	return id, err
}

func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func btoi(b []byte) int {
	return int(binary.BigEndian.Uint64(b))
}
