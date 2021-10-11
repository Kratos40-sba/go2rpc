package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/rpc"
	"os"
	"sync"
)

const saveQueueLength = 1000

type Store interface {
	Put(url, key *string) error
	Get(key, url *string) error
}
type ProxyStore struct {
	urls   *UrlStore // local cche
	client *rpc.Client
}
type record struct {
	Key, Url string
}
type UrlStore struct {
	urls map[string]string
	mu   sync.RWMutex
	save chan record
}

func (s *UrlStore) saveLoop(filename string) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalln("Error opening UrlStore :", err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Println("Error while closing file : ", err)
		}
	}(f)
	e := json.NewEncoder(f)
	for {
		r := <-s.save
		if err := e.Encode(r); err != nil {
			log.Println("Error saving to UrlStore : ", err)
		}
	}
}
func (s *UrlStore) load(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		log.Println("Error opening UrlStore : ", err)
		return err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Println("Error while closing file : ", err)
		}
	}(f)
	d := json.NewDecoder(f)
	for err == nil {
		var r record
		if err = d.Decode(&r); err == nil {
			err := s.Set(&r.Key, &r.Url)
			if err != nil {
				return err
			}
		}
	}
	if err == io.EOF {
		return nil
	}
	log.Println("Error decoding UrlStore : ", err)
	return err
}
func NewUrlStore(fileName string) *UrlStore {
	s := &UrlStore{urls: make(map[string]string)}
	if err := s.load(fileName); err != nil {
		s.save = make(chan record, saveQueueLength)
		log.Println("Error loading UrlStore : ", err)
	}
	go func() {
		err := s.saveLoop(fileName)
		if err != nil {
			log.Println("Error in the goroutine ", err)
		}
	}()
	return s
}
func (s *UrlStore) Get(key, url *string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if u, ok := s.urls[*key]; ok {
		*url = u
		return nil
	}
	return errors.New("key not found")
}
func (s *UrlStore) Set(key, url *string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.urls[*key]; ok {
		return errors.New("key already exists")
	}
	s.urls[*key] = *url
	return nil
}
func (s *UrlStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.urls)
}
func (s *UrlStore) Put(url, key *string) error {
	for {
		*key = genKey(*url)
		if err := s.Set(key, url); err != nil {
			break
		}

	}
	if s.save != nil {
		s.save <- record{*key, *url}
	}
	return nil
}
func NewProxyStore(addr string) *ProxyStore {
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Println("Error constructing ProxyStore: ", err)
	}
	return &ProxyStore{urls: NewUrlStore(""), client: client}
}
func (s *ProxyStore) Get(key, url *string) error {
	if err := s.urls.Get(key, url); err != nil {
		return nil
	}
	// rpc call to master
	if err := s.client.Call("Store.Get", key, url); err != nil {
		return err
	}
	err := s.urls.Set(key, url) // update the cache
	if err != nil {
		return err
	}
	return nil
}
func (s *ProxyStore) Put(url, key *string) error {
	// rpc call to master
	if err := s.client.Call("Store.Put", url, key); err != nil {
		return err
	}
	err := s.urls.Set(key, url) // update local cache
	if err != nil {
		return err
	}
	return nil
}
