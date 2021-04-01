package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

type httpServer struct {
	Log *Log
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func NewHTTPServer(addr string) *http.Server {
	httpServer := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpServer.HandleProduce).Methods(http.MethodPost)
	r.HandleFunc("/", httpServer.HandleConsume).Methods(http.MethodGet)

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

func (s *httpServer) HandleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	if decodeErr := json.NewDecoder(r.Body).Decode(&req); decodeErr != nil {
		http.Error(w, decodeErr.Error(), http.StatusBadRequest)
		return
	}

	record, readErr := s.Log.Read(req.Offset)
	if readErr != nil {
		switch readErr {
		case ErrOffsetNotFound:
			http.Error(w, readErr.Error(), http.StatusNotFound)
			break
		default:
			http.Error(w, readErr.Error(), http.StatusInternalServerError)
			break
		}
	}

	res := ConsumeResponse{Record: record}
	if encodeErr := json.NewEncoder(w).Encode(res); encodeErr != nil {
		http.Error(w, encodeErr.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) HandleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	offset, appendErr := s.Log.Append(req.Record)
	if appendErr != nil {
		http.Error(w, appendErr.Error(), http.StatusInternalServerError)
		return
	}

	res := ProduceResponse{Offset: offset}
	if encodeErr := json.NewEncoder(w).Encode(res); encodeErr != nil {
		http.Error(w, encodeErr.Error(), http.StatusInternalServerError)
		return
	}
}
