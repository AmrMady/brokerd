package api

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.yaml.in/yaml/v3"

	"github.com/AmrMady/brokerd/internal/engine"
	"github.com/AmrMady/brokerd/internal/marketdata"
)

// The OpenAPI spec lists every route the server has, and no others.
func TestOpenAPIMatchesRoutes(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("../../openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err = yaml.Unmarshal(data, &spec); err != nil {
		t.Fatal(err)
	}
	var documented []string
	for path, ops := range spec.Paths {
		for method := range ops {
			documented = append(documented, strings.ToUpper(method)+" "+path)
		}
	}

	// Nothing is called while the routes are set up, so empty values do.
	s, err := New(Config{Pool: &pgxpool.Pool{}, Engine: &engine.Engine{}, Quotes: &marketdata.QuoteCache{}, Bars: marketdata.NewBarStore(1)})
	if err != nil {
		t.Fatal(err)
	}
	served := []string{"GET /v1/stream", "GET /metrics"} // mounted by serve
	for _, r := range s.routes {
		if r != "/" {
			served = append(served, r)
		}
	}
	slices.Sort(documented)
	slices.Sort(served)
	if !slices.Equal(documented, served) {
		t.Errorf("openapi.yaml documents\n%q\nbut the server has\n%q", documented, served)
	}
}
