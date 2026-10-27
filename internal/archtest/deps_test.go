// Package archtest keeps package boundaries honest: each internal package may
// import only the internal packages listed here. Adding an import or a
// package means updating this list on purpose (see docs/PLAN.md).
package archtest

import (
	"os/exec"
	"strings"
	"testing"
)

const module = "github.com/valayash/stepwise/"

// allowed maps each package to the internal packages it may import. Leaves
// (nil) import nothing internal. cmd/ is wiring and may import anything.
var allowed = map[string][]string{
	// leaves
	"internal/breaker":   nil,
	"internal/config":    nil,
	"internal/mcp":       nil,
	"internal/metrics":   nil,
	"internal/provider":  nil,
	"internal/ratelimit": nil,
	"internal/telemetry": nil,

	// domain and storage
	"internal/core":                     {"internal/provider"},
	"internal/store":                    {"internal/core"},
	"internal/store/memory":             {"internal/core", "internal/store"},
	"internal/store/postgres":           {"internal/core", "internal/store"},
	"internal/store/postgres/pgxdriver": {"internal/store/postgres"},
	"internal/store/storetest":          {"internal/core", "internal/provider", "internal/store"},

	// features; edge packages (api, gateway, webhook, engine) never import each other
	"internal/tools":   {"internal/core", "internal/mcp", "internal/telemetry"},
	"internal/demo":    {"internal/mcp"},
	"internal/webhook": {"internal/breaker", "internal/metrics", "internal/store"},
	"internal/gateway": {"internal/provider"},
	"internal/api":     {"internal/core", "internal/provider", "internal/store", "internal/tools"},
	"internal/engine": {"internal/breaker", "internal/core", "internal/metrics", "internal/provider",
		"internal/ratelimit", "internal/store", "internal/telemetry", "internal/tools"},
}

func TestPackageDependencies(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", `{{.ImportPath}} {{join .Imports " "}}`, module+"...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(strings.ReplaceAll(line, module, ""))
		pkg, imports := fields[0], fields[1:]
		if strings.HasPrefix(pkg, "cmd/") || pkg == "internal/archtest" {
			continue
		}
		ok, known := allowed[pkg]
		if !known {
			t.Errorf("%s: new package; add it to allowed in archtest", pkg)
			continue
		}
		for _, imp := range imports {
			if strings.HasPrefix(imp, "internal/") && !contains(ok, imp) {
				t.Errorf("%s must not import %s", pkg, imp)
			}
		}
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
