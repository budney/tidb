package cluster

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sort"
	"strings"

	"golang.org/x/exp/slices"
)

// This constant specifies a threshold for dependencies that
// are just too common and that should be ignored.
const maxDepFrequency = 0.50

// This setting caps the size of any one cluster, as a crude
// method of preventing giant super-clusters.
const maxClusterSize = 25

// clusterPackages groups the given packages into clusters based on shared dependencies.
func ClusterPackages(pkgs []string) [][]string {
	deps, freqs, err := collectDependencies(pkgs)
	if err != nil {
		log.Fatalf("failed to collect dependencies: %v", err)
	}

	edges := buildEdges(deps, freqs)

	// Initialize disjoint set
	parent := make(map[string]string)
	size := make(map[string]int)

	for _, p := range pkgs {
		parent[p] = p
		size[p] = 1
	}

	var find func(string) string
	find = func(x string) string {
		for parent[x] != x {
			parent[x] = parent[parent[x]] // path compression
			x = parent[x]
		}
		return x
	}

	var union func(string, string)
	union = func(x, y string) {
		xRoot := find(x)
		yRoot := find(y)
		if xRoot == yRoot {
			return
		}
		// Merge yRoot into xRoot
		parent[yRoot] = xRoot
		size[xRoot] += size[yRoot]
		delete(size, yRoot) // optional: keeps size map clean
	}

	for _, e := range edges {
		aRoot := find(e.a)
		bRoot := find(e.b)
		if aRoot == bRoot {
			continue
		}
		aSize := size[aRoot]
		bSize := size[bRoot]

		if aSize+bSize <= maxClusterSize {
			union(aRoot, bRoot)
		}
	}

	// Collect final clusters
	clusters := make(map[string][]string)
	for _, p := range pkgs {
		root := find(p)
		clusters[root] = append(clusters[root], p)
	}

	// Format result
	var result [][]string
	for _, group := range clusters {
		result = append(result, group)
	}

	// Sort to get biggest groups first
	sort.Slice(result, func(i, j int) bool {
		return len(result[i]) < len(result[j])
	})
	slices.Reverse(result)

	return result
}

type edge struct {
	a, b   string
	weight int
}

func getModulePrefix() (string, error) {
	f, err := os.Open("go.mod")
	if err != nil {
		return "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "module ") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				return fields[1] + "/", nil
			}
		}
	}
	return "", fmt.Errorf("module line not found in go.mod")
}

// collectDependnecies calls "go list" to list all dependencies of each package in
// the project, and returns a map of packages to dependencies that can be used
// to look for overlapping dependencies between packages.
func collectDependencies(pkgs []string) (
	map[string]map[string]struct{},
	map[string]int,
	error,
) {
	modulePrefix, err := getModulePrefix()
	if err != nil {
		log.Fatalf("failed to get module prefix: %v", err)
	}

	cmd := exec.Command("go", "list", "-f", "{{.ImportPath}}:{{.Deps}}", "./...")
	out, err := cmd.Output()
	if err != nil {
		return nil, nil, fmt.Errorf("go list failed: %w", err)
	}

	deps := make(map[string]map[string]struct{})
	depFrequency := make(map[string]int)

	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		pkg := strings.TrimPrefix(strings.TrimSpace(parts[0]), modulePrefix)
		depSet := make(map[string]struct{})

		for _, dep := range strings.Fields(parts[1]) {
			var trimmedDep string = dep

			// trim the module prefix; otherwise leave alone
			if strings.HasPrefix(dep, modulePrefix) {
				trimmedDep = strings.TrimPrefix(dep, modulePrefix)
			}

			depSet[trimmedDep] = struct{}{}
			depFrequency[trimmedDep]++
		}

		deps[pkg] = depSet
	}

	return deps, depFrequency, nil
}

func buildEdges(deps map[string]map[string]struct{}, freqs map[string]int) []edge {
	var edges []edge
	var pkgs []string
	for p := range deps {
		pkgs = append(pkgs, p)
	}
	for i := 0; i < len(pkgs); i++ {
		for j := i + 1; j < len(pkgs); j++ {
			a, b := pkgs[i], pkgs[j]
			shared := countFilteredOverlap(deps[a], deps[b], freqs, int(maxDepFrequency*float64(len(pkgs))))
			if shared > 0 {
				edges = append(edges, edge{a, b, shared})
			}
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		return edges[i].weight > edges[j].weight
	})
	return edges
}

// countOverlap counts all overlapping dependencies between
// two modules.
func countOverlap(a, b map[string]struct{}) int {
	count := 0
	for k := range a {
		if _, ok := b[k]; ok {
			count++
		}
	}
	return count
}

// countFilteredOverlap counts the overlapping dpendencies between
// two modules, ignoring dependencies that are common to more than
// the stated maximum number of packages. This avoids packages all
// ending up in one giant cluster because they all depend on fmt
// or something.
func countFilteredOverlap(a, b map[string]struct{}, freq map[string]int, maxAllowed int) int {
	count := 0
	for k := range a {
		if _, ok := b[k]; ok && freq[k] <= maxAllowed {
			count++
		}
	}
	return count
}
