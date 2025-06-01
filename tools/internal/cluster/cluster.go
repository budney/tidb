package cluster

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/exp/slices"
)

var modulePath = filepath.Join("github.com", "pingcap", "tidb")

// maxDepFrequency specifies a threshold for dependencies that are just
// too common and that should be ignored.
const maxDepFrequency = 0.50

// maxClusterSize caps the size of any one cluster, as a crude method
// of preventing giant super-clusters. This number was experimentally
// arrived at to keep disk usage around 10GB or less.
const maxClusterSize = 37

// clusterPackages groups the given packages into clusters based on shared dependencies.
// It uses agglomerative clustering based on the number of dependencies in common, limited
// by maxClusterSize. The resulting clusters are sorted by decreasing size, and then
// merged again in order to yield a minimal set of clusters.
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

	// Sort by decreasing size
	sort.Slice(result, func(i, j int) bool {
		return len(result[i]) < len(result[j])
	})
	slices.Reverse(result)

	// Finally, consolidate clusters by first-fit
	result = consolidate(result)

	return result
}

// consolidate combines arrays of strings by putting each array into
// the first bin that has room for it without exceeding the maximum size
// limit. The arrays are assimed to be sorted by descending size. This
// will yield a result within about 22% of the minimal number of bins.
func consolidate(clusters [][]string) [][]string {
	merged := make([][]string, 0)

	for _, cluster := range clusters {
		placed := false
		for j, bin := range merged {
			if len(bin)+len(cluster) <= maxClusterSize {
				merged[j] = append(bin, cluster...)
				placed = true
				break
			}
		}
		if !placed {
			merged = append(merged, make([]string, 0))
		}
	}

	return merged
}

type edge struct {
	a, b   string
	weight int
}

// collectDependnecies calls "go list" to list all dependencies of each
// package in the project, and returns a map of packages to dependencies
// that can be used to count the overlapping dependencies. This gives
// a weight for the graph we're building.
func collectDependencies(pkgs []string) (
	map[string]map[string]struct{},
	map[string]int,
	error,
) {
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

		pkg := strings.TrimPrefix(strings.TrimSpace(parts[0]), modulePath)
		depSet := make(map[string]struct{})

		for _, dep := range strings.Fields(parts[1]) {
			var trimmedDep string = dep

			// trim the module prefix; otherwise leave alone
			if strings.HasPrefix(dep, modulePath) {
				trimmedDep = strings.TrimPrefix(dep, modulePath)
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

	// copy the array of packages
	for p := range deps {
		pkgs = append(pkgs, p)
	}

	// create a graph weighted by the number of shared dependencies
	for i := range pkgs {
		for j := i + 1; j < len(pkgs); j++ {
			a, b := pkgs[i], pkgs[j]
			shared := countFilteredOverlap(deps[a], deps[b], freqs, int(maxDepFrequency*float64(len(pkgs))))
			if shared > 0 {
				edges = append(edges, edge{a, b, shared})
			}
		}
	}

	// sort the edges by weight for agglomerative clustering
	sort.Slice(edges, func(i, j int) bool {
		return edges[i].weight > edges[j].weight
	})

	return edges
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
