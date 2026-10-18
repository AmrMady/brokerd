package api

import "net/http"

// WriteErrorForTest lets tests outside the package write an error response.
func (s *Server) WriteErrorForTest(w http.ResponseWriter, r *http.Request, err error) {
	s.writeError(w, r, err)
}
