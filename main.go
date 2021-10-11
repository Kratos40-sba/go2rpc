package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
)

var (
	listenAddr = flag.String("http", ":3000", "http listen address")
	dataFile   = flag.String("file", "store.json", "data store file name")
	masterAddr = flag.String("master", "", "RPC master address")
	rpcEnabled = flag.Bool("rpc", false, "enable RPC server")
)

const addForm = `
<html><body>
<form method="POST" action="/add">
URL : <input type="text" name="url">
<input type="submit" value="add">
</form>
</body></html>
`

var store Store

func main() {
	flag.Parse()
	if *masterAddr != "" {
		store = NewProxyStore(*masterAddr)
	} else {
		store = NewUrlStore(*dataFile)
	}
	if *rpcEnabled {
		err := rpc.RegisterName("Store", store)
		if err != nil {
			log.Println("RPC ERROR : ", err)
			return
		}
		rpc.HandleHTTP()
	}
	http.HandleFunc("/", Redirect)
	http.HandleFunc("/add", Add)
	log.Fatalln(http.ListenAndServe(*listenAddr, nil))
}
func Add(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	url := r.FormValue("url")
	if url == "" {
		fmt.Fprint(w, addForm)
		return
	}
	var key string
	if err := store.Put(&url, &key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "%s", key)
}
func Redirect(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[1:]
	if key == "" {
		http.NotFound(w, r)
		return
	}
	var url string
	if err := store.Get(&key, &url); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, url, http.StatusFound)
}
