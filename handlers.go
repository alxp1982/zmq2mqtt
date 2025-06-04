package main

import (
	"encoding/json"
	"net/http"
	"text/template"
)

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	tmpl := template.Must(template.ParseFiles("templates/dashboard.html"))
	tmpl.Execute(w, nil)
}

func handleStats(w http.ResponseWriter, r *http.Request, stats *Stats) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats.GetStats())
}
