// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/tools/internal/cluster"

	// Set the correct value when it runs inside docker.
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sync/errgroup"
	"golang.org/x/tools/cover"
)

func usage() bool {
	msg := `// run all tests
ut

// show usage
ut -h

// list all packages
ut list

// list test cases of a single package
ut list $package

// list test cases that match a pattern
ut list $package 'r:$regex'

// run all tests
ut run

// run test all cases of a single package
ut run $package

// run test cases of a single package
ut run $package $test

// run test cases that match a pattern
ut run $package 'r:$regex'

// run test cases of multiple packages
ut run-multi $package1 $package2 ...

// build all test package
ut build

// build a test package
ut build xxx

// run in tight-cluster mode: compile and run clusters separately, clearing cache between.
// used to save disk space
ut run --tight

// write the junitfile
ut run --junitfile xxx

// test with race flag
ut run --race

// test with test.short flag
ut run --short

// test with long flag
// when the '--long' flag is set, ut will only run the long tests and have different strategies for concurreny to make them stabler.
ut run --long`

	fmt.Println(msg)
	return true
}

var modulePath = filepath.Join("github.com", "pingcap", "tidb")

type task struct {
	pkg  string
	test string
}

func (t *task) String() string {
	return t.pkg + " " + t.test
}

var p int
var buildParallel int
var workDir string
var tight bool

func cmdList(args ...string) bool {
	pkgs, err := listPackages()
	if err != nil {
		log.Println("list package error", err)
		return false
	}

	// list all packages
	if len(args) == 0 {
		if tight {
			clusters := cluster.ClusterPackages(pkgs)
			for i, cluster := range clusters {
				fmt.Printf("Cluster %d:\n", i+1)
				for _, pkg := range cluster {
					fmt.Printf("  %s\n", pkg)
				}
				fmt.Println()
			}
		} else {
			for _, pkg := range pkgs {
				fmt.Println(pkg)
			}
		}
		return false
	}

	// list test case of a single package
	if len(args) == 1 || len(args) == 2 {
		pkg := args[0]
		pkgs = filter(pkgs, func(s string) bool { return s == pkg })
		if len(pkgs) != 1 {
			fmt.Println("package not exist", pkg)
			return false
		}

		err := buildTestBinary(pkg)
		if err != nil {
			log.Println("build package error", pkg, err)
			return false
		}
		exist, err := testBinaryExist(pkg)
		if err != nil {
			log.Println("check test binary existence error", err)
			return false
		}
		if !exist {
			fmt.Println("no test case in ", pkg)
			return false
		}

		res, err := listTestCases(nil, pkg)
		if err != nil {
			log.Println("list test cases for package error", err)
			return false
		}

		if len(args) == 2 {
			res, err = filterTestCases(res, args[1])
			if err != nil {
				fmt.Println("filter test cases error", err)
				return false
			}
		}

		for _, x := range res {
			fmt.Println(x.test)
		}
	}
	return true
}

func cmdBuild(args ...string) bool {
	pkgs, err := listPackages()
	if err != nil {
		log.Println("list package error", err)
		return false
	}

	// build all packages
	if len(args) == 0 {
		if tight {
			// build one cluster at a time, clearing the cache between
			if err := buildTightClusterMulti(pkgs); err != nil {
				log.Println("build in cluster mode failed", err)
				return false
			}
			return true
		} else {
			// build *all* the test packages
			if err := buildTestBinaryMulti(pkgs); err != nil {
				log.Println("build package error", pkgs, err)
				return false
			}
		}
	}

	// build test binary of a single package
	if len(args) >= 1 {
		pkg := args[0]
		err := buildTestBinary(pkg)
		if err != nil {
			log.Println("build package error", pkg, err)
			return false
		}
	}
	return true
}

// buildAndRunTests handles all of the permutations of "run" command.
// It will build tests for the specified packages, filter cases based
// on various criteria, run the tests, and return success or failure.
func buildAndRunTests(pkgs []string, onlyCases, exceptCases map[string]struct{}, matching string) bool {
	var err error

	// if `-long` flag is set, only build long tests and run them
	if long {
		filtered := make([]string, 0)
		for _, pkg := range pkgs {
			if _, ok := longTests[pkg]; ok {
				filtered = append(filtered, pkg)
			}
		}
		pkgs = filtered
	}

	// if there are no tests to run, then they all pass
	if len(pkgs) == 0 {
		return true
	}

	start := time.Now()

	// build ALL the packages requested
	err = buildTestBinaryMulti(pkgs)
	if err != nil {
		log.Println("build package error", pkgs, err)
		return false
	}

	var tasks []task

	// list all the test cases in the packages we built
	tasks, err = listTestCasesForPkgs(pkgs)
	if err != nil {
		log.Println("run existing test cases error", err)
		return false
	}

	if len(tasks) == 0 {
		// this seems unlikely, but still
		log.Println("no test cases found to run")
		return true
	}

	if long {
		longTasks := make(map[string]struct{})

		// only execute the "long" test cases
		for _, t := range listLongTasks(tasks, pkgs...) {
			// intersected with the restriction already specified, if any
			if _, ok := onlyCases[t.String()]; onlyCases != nil && !ok {
				continue
			}
			longTasks[t.String()] = struct{}{}
		}

		onlyCases = longTasks
	}

	// exclude the specified cases
	if exceptCases != nil {
		tmp := tasks[:0]
		for _, task := range tasks {
			if _, ok := exceptCases[task.String()]; !ok {
				tmp = append(tmp, task)
			}
		}
		tasks = tmp
	}

	// restrict to the specified cases
	if onlyCases != nil {
		tmp := tasks[:0]
		for _, task := range tasks {
			if _, ok := onlyCases[task.String()]; ok {
				tmp = append(tmp, task)
			}
		}
		tasks = tmp
	}

	// also limit it to matching cases
	if matching != "" {
		tasks, err = filterTestCases(tasks, matching)
		if err != nil {
			log.Println("filter test cases error", err)
			return false
		}
	}

	fmt.Printf("building task finish, maxproc=%d, count=%d, takes=%v\n", buildParallel, len(tasks), time.Since(start))
	return runTestCases(tasks)
}

func cmdRunMulti(pkgs ...string) bool {
	if len(pkgs) == 0 {
		return true
	}

	if len(pkgs) <= 2 {
		return buildAndRunTests(pkgs, nil, nil, "")
	}

	// delegate to cmdRun, which implements more options
	return cmdRun(pkgs...)
}

func cmdRun(args ...string) bool {
	var err error
	var pkgs []string

	var exceptCases map[string]struct{}
	var onlyCases map[string]struct{}
	var matching string

	if except != "" {
		exceptCases, err = parseCaseListFromFile(except)
		if err != nil {
			log.Println("parse --except file error", err)
			return false
		}
	}

	if only != "" {
		onlyCases, err = parseCaseListFromFile(only)
		if err != nil {
			log.Println("parse --only file error", err)
			return false
		}
	}

	// run tests for a single package
	if len(args) == 1 {
		pkgs = []string{args[0]}
		return buildAndRunTests(pkgs, onlyCases, exceptCases, matching)
	}

	// run matching test(s) in a single package
	if len(args) == 2 {
		pkgs = []string{args[0]}
		matching = args[1]

		return buildAndRunTests(pkgs, onlyCases, exceptCases, matching)
	}

	if len(args) == 0 {
		// "run" with no args means shoot the works
		pkgs, err = listPackages()
		if err != nil {
			fmt.Println("list packages error", err)
			return false
		}
	} else {
		// more than 2 args means "run this list"
		// it's a synonym for run-multi
		pkgs = args
	}

	if tight {
		clusters := cluster.ClusterPackages(pkgs) // some chunking logic
		isSuccess := true
		start := time.Now()

		for i, cluster := range clusters {
			if !buildAndRunTests(cluster, onlyCases, exceptCases, matching) {
				isSuccess = false
			}

			if err := runCmd("make", "testclean"); err != nil {
				log.Printf("cache clean failed for cluster %d: %v", i+1, err)
			}
		}

		fmt.Printf("build and run task finish, parallelism=%d, batches=%d, takes=%v\n", buildParallel, len(clusters), time.Since(start))
		return isSuccess
	} else {
		return buildAndRunTests(pkgs, onlyCases, exceptCases, matching)
	}

	// execution can't reach this point
}

func runTestCases(tasks []task) bool {
	testWorkerCount := p
	if long {
		testWorkerCount = longTestWorkerCount
	}
	taskCh := make(chan task, 100)
	works := make([]numa, testWorkerCount)
	var wg sync.WaitGroup
	for i := range testWorkerCount {
		wg.Add(1)
		go works[i].worker(&wg, taskCh)
	}

	shuffle(tasks)

	start := time.Now()
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	wg.Wait()
	fmt.Println("run all tasks takes", time.Since(start))

	if junitfile != "" {
		out := collectTestResults(works)
		f, err := os.Create(junitfile)
		if err != nil {
			fmt.Println("create junit file fail:", err)
			return false
		}
		defer f.Close()
		if err := write(f, out); err != nil {
			fmt.Println("write junit file error:", err)
			return false
		}
	}

	if coverprofile != "" {
		collectCoverProfileFile()
	}

	for _, work := range works {
		if work.Fail {
			return false
		}
	}
	return true
}

func listTestCasesForPkgs(pkgs []string) (tasks []task, err error) {
	g := new(errgroup.Group)
	tasksChannel := make(chan []task, len(pkgs))
	for _, pkg := range pkgs {
		exist, err := testBinaryExist(pkg)
		if err != nil {
			log.Println("check test binary existence error", err)
			return nil, err
		}
		if !exist {
			fmt.Println("no test case in ", pkg)
			continue
		}

		pkgCopy := pkg
		g.Go(func() error {
			tasks, err := listTestCases(nil, pkgCopy)
			if err != nil {
				log.Println("list test cases error", pkgCopy, err)
				return withTrace(err)
			}
			tasksChannel <- tasks
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, withTrace(err)
	}

	close(tasksChannel)
	for t := range tasksChannel {
		tasks = append(tasks, t...)
	}
	return tasks, nil
}

func parseCaseListFromFile(fileName string) (map[string]struct{}, error) {
	ret := make(map[string]struct{})

	f, err := os.Open(filepath.Clean(fileName))
	if os.IsNotExist(err) {
		return ret, nil
	}
	if err != nil {
		return nil, withTrace(err)
	}
	//nolint: errcheck
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Bytes()
		ret[string(line)] = struct{}{}
	}
	if err := s.Err(); err != nil {
		return nil, withTrace(err)
	}
	return ret, nil
}

// handleFlags strip the '--flag xxx' from the command line os.Args
// Example of the os.Args changes
// Before: ut run sessoin TestXXX --coverprofile xxx --junitfile yyy
// After: ut run session TestXXX
// The value of the flag is returned.
func handleFlags(flag string) string {
	var res string
	tmp := os.Args[:0]
	// Iter to the flag
	var i int
	for ; i < len(os.Args); i++ {
		if os.Args[i] == flag {
			i++
			break
		}
		tmp = append(tmp, os.Args[i])
	}
	// Handle the flag
	if i < len(os.Args) {
		res = os.Args[i]
		i++
	}
	// Iter the remain flags
	for ; i < len(os.Args); i++ {
		tmp = append(tmp, os.Args[i])
	}

	// os.Args is now the original flags with '--coverprofile XXX' removed.
	os.Args = tmp
	return res
}

func handleFlag(f string) (found bool) {
	tmp := os.Args[:0]
	for i := range len(os.Args) {
		if os.Args[i] == f {
			found = true
			continue
		}
		tmp = append(tmp, os.Args[i])
	}
	os.Args = tmp
	return
}

var junitfile string
var coverprofile string
var coverFileTempDir string
var race bool
var short bool
var long bool

var except string
var only string

//nolint:typecheck
func main() {
	junitfile = handleFlags("--junitfile")
	coverprofile = handleFlags("--coverprofile")
	tight = handleFlag("--tight")
	except = handleFlags("--except")
	only = handleFlags("--only")
	race = handleFlag("--race")
	short = handleFlag("--short")
	long = handleFlag("--long")

	if coverprofile != "" {
		var err error
		coverFileTempDir, err = os.MkdirTemp(os.TempDir(), "cov")
		if err != nil {
			fmt.Println("create temp dir fail", coverFileTempDir)
			os.Exit(1)
		}
		defer os.Remove(coverFileTempDir)
	}

	// Get the correct count of CPU if it's in docker.
	p = runtime.GOMAXPROCS(0)
	// We use 2 * p for `go build` to make it faster.
	buildParallel = p * 2
	var err error
	workDir, err = os.Getwd()
	if err != nil {
		fmt.Println("os.Getwd() error", err)
		os.Exit(1)
	}

	var isSucceed bool
	if len(os.Args) == 1 {
		// run all tests
		isSucceed = cmdRun()
	}

	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "list":
			isSucceed = cmdList(os.Args[2:]...)
		case "build":
			isSucceed = cmdBuild(os.Args[2:]...)
		case "run":
			isSucceed = cmdRun(os.Args[2:]...)
		case "run-multi":
			isSucceed = cmdRunMulti(os.Args[2:]...)
		default:
			isSucceed = usage()
		}
	}
	if !isSucceed {
		os.Exit(1)
	}
}

func collectCoverProfileFile() {
	// Combine all the cover file of single test function into a whole.
	files, err := os.ReadDir(coverFileTempDir)
	if err != nil {
		fmt.Println("collect cover file error:", err)
		os.Exit(-1)
	}

	w, err := os.Create(coverprofile)
	if err != nil {
		fmt.Println("create cover file error:", err)
		os.Exit(-1)
	}
	//nolint: errcheck
	defer w.Close()
	w.WriteString("mode: set\n")

	result := make(map[string]*cover.Profile)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		collectOneCoverProfileFile(result, file)
	}

	w1 := bufio.NewWriter(w)
	for _, prof := range result {
		for _, block := range prof.Blocks {
			fmt.Fprintf(w1, "%s:%d.%d,%d.%d %d %d\n",
				prof.FileName,
				block.StartLine,
				block.StartCol,
				block.EndLine,
				block.EndCol,
				block.NumStmt,
				block.Count,
			)
		}
		if err := w1.Flush(); err != nil {
			fmt.Println("flush data to cover profile file error:", err)
			os.Exit(-1)
		}
	}
}

func collectOneCoverProfileFile(result map[string]*cover.Profile, file os.DirEntry) {
	f, err := os.Open(filepath.Join(coverFileTempDir, file.Name()))
	if err != nil {
		fmt.Println("open temp cover file error:", err)
		os.Exit(-1)
	}
	//nolint: errcheck
	defer f.Close()

	profs, err := cover.ParseProfilesFromReader(f)
	if err != nil {
		fmt.Println("parse cover profile file error:", err)
		os.Exit(-1)
	}
	mergeProfile(result, profs)
}

func mergeProfile(m map[string]*cover.Profile, profs []*cover.Profile) {
	for _, prof := range profs {
		sort.Sort(blocksByStart(prof.Blocks))
		old, ok := m[prof.FileName]
		if !ok {
			m[prof.FileName] = prof
			continue
		}

		// Merge samples from the same location.
		// The data has already been sorted.
		tmp := old.Blocks[:0]
		var i, j int
		for i < len(old.Blocks) && j < len(prof.Blocks) {
			v1 := old.Blocks[i]
			v2 := prof.Blocks[j]

			switch compareProfileBlock(v1, v2) {
			case -1:
				tmp = appendWithReduce(tmp, v1)
				i++
			case 1:
				tmp = appendWithReduce(tmp, v2)
				j++
			default:
				tmp = appendWithReduce(tmp, v1)
				tmp = appendWithReduce(tmp, v2)
				i++
				j++
			}
		}
		for ; i < len(old.Blocks); i++ {
			tmp = appendWithReduce(tmp, old.Blocks[i])
		}
		for ; j < len(prof.Blocks); j++ {
			tmp = appendWithReduce(tmp, prof.Blocks[j])
		}

		m[prof.FileName] = old
	}
}

// appendWithReduce works like append(), but it merge the duplicated values.
func appendWithReduce(input []cover.ProfileBlock, b cover.ProfileBlock) []cover.ProfileBlock {
	if len(input) >= 1 {
		last := &input[len(input)-1]
		if b.StartLine == last.StartLine &&
			b.StartCol == last.StartCol &&
			b.EndLine == last.EndLine &&
			b.EndCol == last.EndCol {
			if b.NumStmt != last.NumStmt {
				panic(fmt.Errorf("inconsistent NumStmt: changed from %d to %d", last.NumStmt, b.NumStmt))
			}
			// Merge the data with the last one of the slice.
			last.Count |= b.Count
			return input
		}
	}
	return append(input, b)
}

type blocksByStart []cover.ProfileBlock

func compareProfileBlock(x, y cover.ProfileBlock) int {
	if x.StartLine < y.StartLine {
		return -1
	}
	if x.StartLine > y.StartLine {
		return 1
	}

	// Now x.StartLine == y.StartLine
	if x.StartCol < y.StartCol {
		return -1
	}
	if x.StartCol > y.StartCol {
		return 1
	}

	return 0
}

func (b blocksByStart) Len() int      { return len(b) }
func (b blocksByStart) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b blocksByStart) Less(i, j int) bool {
	bi, bj := b[i], b[j]
	return bi.StartLine < bj.StartLine || bi.StartLine == bj.StartLine && bi.StartCol < bj.StartCol
}

// listTestCases list all test cases of a package and append to a slice.
func listTestCases(tasks []task, pkgs ...string) ([]task, error) {
	for _, pkg := range pkgs {
		newCases, err := listNewTestCases(pkg)
		if err != nil {
			log.Println("list test case error", pkg, err)
			return nil, withTrace(err)
		}
		for _, c := range newCases {
			tasks = append(tasks, task{pkg, c})
		}
	}

	return tasks, nil
}

func filterTestCases(tasks []task, arg1 string) ([]task, error) {
	if strings.HasPrefix(arg1, "r:") {
		r, err := regexp.Compile(arg1[2:])
		if err != nil {
			return nil, err
		}
		tmp := tasks[:0]
		for _, task := range tasks {
			if r.MatchString(task.test) {
				tmp = append(tmp, task)
			}
		}
		return tmp, nil
	}
	tmp := tasks[:0]
	for _, task := range tasks {
		if strings.Contains(task.test, arg1) {
			tmp = append(tmp, task)
		}
	}
	return tmp, nil
}

func listLongTasks(tasks []task, pkgs ...string) []task {
	for _, pkg := range pkgs {
		for _, t := range longTests[pkg] {
			tasks = append(tasks, task{pkg, t})
		}
	}
	return tasks
}

func listPackages() ([]string, error) {
	listPath := strings.Join([]string{".", "..."}, string(filepath.Separator))
	cmd := exec.Command("go", "list", listPath)
	ss, err := cmdToLines(cmd)
	if err != nil {
		return nil, withTrace(err)
	}

	ret := ss[:0]
	for _, s := range ss {
		if !strings.HasPrefix(s, modulePath) {
			continue
		}
		pkg := s[len(modulePath)+1:]
		if skipDIR(pkg) {
			continue
		}
		ret = append(ret, pkg)
	}
	return ret, nil
}

type numa struct {
	Fail    bool
	results []testResult
}

func (n *numa) worker(wg *sync.WaitGroup, ch chan task) {
	defer wg.Done()
	for t := range ch {
		res := n.runTestCase(t.pkg, t.test)
		if res.Failure != nil {
			fmt.Println("[FAIL] ", t.pkg, t.test)
			fmt.Fprintf(os.Stderr, "err=%s\n%s", res.err.Error(), res.Failure.Contents)
			n.Fail = true
		}
		n.results = append(n.results, res)
	}
}

type testResult struct {
	JUnitTestCase
	d   time.Duration
	err error
}

// makeGoTmpDir accepts a command and a pre-filled testResult, and
// returns a cleanup function and error if any.
func makeGoTmpDir(cmd *exec.Cmd) (func(), error) {
	// Capture the original GOTMPDIR
	baseTmp := os.Getenv("GOTMPDIR")
	if baseTmp == "" {
		return func() {}, nil // no-op
	}

	// Make a new GOTMPDIR inside the original
	tmpSubdir, err := os.MkdirTemp(baseTmp, "gotmp-")
	if err != nil {
		return nil, err
	}

	// Set GOTMPDIR for the test subprocess
	cmd.Env = append(os.Environ(), "GOTMPDIR="+tmpSubdir)

	return func() { _ = os.RemoveAll(tmpSubdir) }, nil
}

func (n *numa) runTestCase(pkg string, fn string) testResult {
	res := testResult{
		JUnitTestCase: JUnitTestCase{
			Classname: filepath.Join(modulePath, pkg),
			Name:      fn,
		},
	}

	var buf bytes.Buffer
	var err error
	var start time.Time

	for range 3 {
		cmd := n.testCommand(pkg, fn)
		cmd.Dir = filepath.Join(workDir, pkg)
		cmd.Stdout = &buf
		cmd.Stderr = &buf

		if short {
			cmd.Args = append(cmd.Args, "--test.short")
		}
		if long {
			cmd.Args = append(cmd.Args, "-long")
		}

		var cleanup func()

		// Put build files in an ephemeral directory
		cleanup, err = makeGoTmpDir(cmd)
		if err != nil {
			res.Failure = &JUnitFailure{
				Message:  "Failed to create GOTMPDIR subdir",
				Contents: err.Error(),
			}
			res.err = err
			return res
		}

		start = time.Now()
		err = cmd.Run()
		cleanup()

		if err != nil {
			if _, ok := err.(*exec.ExitError); ok {
				switch err.Error() {
				case "signal: segmentation fault (core dumped)",
					"signal: trace/breakpoint trap (core dumped)":
					buf.Reset()
					continue
				}
				if strings.Contains(buf.String(), "panic during panic") {
					buf.Reset()
					continue
				}
			}
		}
		break
	}

	if err != nil {
		res.Failure = &JUnitFailure{
			Message:  "Failed",
			Contents: buf.String(),
		}
		res.err = err
	}
	res.d = time.Since(start)
	res.Time = formatDurationAsSeconds(res.d)
	return res
}

func collectTestResults(workers []numa) JUnitTestSuites {
	version := goVersion()
	// pkg => test cases
	pkgs := make(map[string][]JUnitTestCase)
	durations := make(map[string]time.Duration)

	// The test result in workers are shuffled, so group by the packages here
	for _, n := range workers {
		for _, res := range n.results {
			cases, ok := pkgs[res.Classname]
			if !ok {
				cases = make([]JUnitTestCase, 0, 10)
			}
			cases = append(cases, res.JUnitTestCase)
			pkgs[res.Classname] = cases
			durations[res.Classname] = durations[res.Classname] + res.d
		}
	}

	suites := JUnitTestSuites{}
	// Turn every package result to a suite.
	for pkg, cases := range pkgs {
		suite := JUnitTestSuite{
			Tests:      len(cases),
			Failures:   failureCases(cases),
			Time:       formatDurationAsSeconds(durations[pkg]),
			Name:       pkg,
			Properties: packageProperties(version),
			TestCases:  cases,
		}
		suites.Suites = append(suites.Suites, suite)
	}
	return suites
}

func failureCases(input []JUnitTestCase) int {
	sum := 0
	for _, v := range input {
		if v.Failure != nil {
			sum++
		}
	}
	return sum
}

func (n *numa) testCommand(pkg string, fn string) *exec.Cmd {
	args := make([]string, 0, 10)
	exe := strings.Join([]string{".", testFileName(pkg)}, string(filepath.Separator))

	if coverprofile != "" {
		fileName := strings.ReplaceAll(pkg, string(filepath.Separator), "_") + "." + fn
		tmpFile := filepath.Join(coverFileTempDir, fileName)
		args = append(args, "-test.coverprofile", tmpFile)
	}
	// for long test, gives it more CPU resources for each test and limit the parallelism.
	testCPU := 1
	if long && p > longTestWorkerCount {
		testCPU = p / longTestWorkerCount
	}
	args = append(args, "-test.cpu", strconv.Itoa(testCPU))
	if !race && !long {
		args = append(args, []string{"-test.timeout", "2m"}...)
	} else {
		// it takes a longer when race is enabled. so it is set more timeout value.
		args = append(args, []string{"-test.timeout", "30m"}...)
	}

	// session.test -test.run TestClusteredPrefixColum
	args = append(args, "-test.run", "^"+fn+"$")

	return exec.Command(exe, args...)
}

func skipDIR(pkg string) bool {
	skipDir := []string{"br", "lightning", filepath.Join("pkg", "lightning"),
		"cmd", "dumpling", "tests", filepath.Join("tools", "check"), "build"}
	for _, ignore := range skipDir {
		if strings.HasPrefix(pkg, ignore) {
			return true
		}
	}
	return false
}

func buildTestBinary(pkg string) error {
	// go test -c
	cmd := exec.Command("go", "test", "-c", "-vet", "off", "--tags=intest", "-o", testFileName(pkg))
	if coverprofile != "" {
		cmd.Args = append(cmd.Args, "-cover")
	}
	if race {
		cmd.Args = append(cmd.Args, "-race")
	}
	if short {
		cmd.Args = append(cmd.Args, "--test.short")
	}
	cmd.Dir = filepath.Join(workDir, pkg)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return withTrace(err)
	}
	return nil
}

func generateBuildCache(pkgs []string) error {
	packages := make([]string, 0)

	// convert the packages into the form needed by go test. The
	// packages are relative to the repository root, two
	// directories up.
	for _, pkg := range pkgs {
		packages = append(packages, "../../"+pkg)
	}

	// cd cmd/tidb-server && go test -tags intest -exec true -vet off -toolexec=go-compile-without-link
	cmd := exec.Command("go", "test", "-tags=intest", "-exec=true", "-vet=off")
	cmd.Dir = filepath.Join(workDir, "cmd", "tidb-server")

	goCompileWithoutLink := fmt.Sprintf("-toolexec=%s", filepath.Join(workDir, "tools", "check", "go-compile-without-link.sh"))
	cmd.Args = append(cmd.Args, goCompileWithoutLink)
	cmd.Args = append(cmd.Args, packages...) // Add the target pakcages

	if err := cmd.Run(); err != nil {
		return withTrace(err)
	}
	return nil
}

// runCmd is a helper for running a command line
func runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Dir = getRepoRoot() // Always run commands in the root directory
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func getRepoRoot() string {
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	if err != nil {
		log.Fatalf("failed to find repo root: %v", err)
	}
	repoRoot := strings.TrimSpace(string(out))

	return repoRoot
}

// buildTightClusterMulti speeds up test building by grouping builds,
// while also saving disk space by clearing the build cache between
// clusters.
func buildTightClusterMulti(pkgs []string) error {
	clusters := cluster.ClusterPackages(pkgs) // same chunking logic

	for i, cluster := range clusters {
		log.Printf("Building cluster %d of %d with %d packages", i+1, len(clusters), len(cluster))
		if err := buildTestBinaryMulti(cluster); err != nil {
			return fmt.Errorf("build failed for cluster %d: %w", i+1, err)
		}
		if err := runCmd("go", "clean", "-cache"); err != nil {
			return fmt.Errorf("cache clean failed for cluster %d: %w", i+1, err)
		}
	}
	return nil
}

// buildTestBinaryMulti is much faster than build the test packages one by one.
func buildTestBinaryMulti(pkgs []string) error {
	// staged build, generate the build cache for all the tests first, then generate the test binary.
	// This way is faster than generating test binaries directly, because the cache can be used.
	if err := generateBuildCache(pkgs); err != nil {
		return withTrace(err)
	}

	// go test --exec=xprog -cover -vet=off --count=0 $(pkgs)
	xprogPath := filepath.Join(workDir, "tools", "bin", "xprog")
	packages := make([]string, 0, len(pkgs))
	for _, pkg := range pkgs {
		packages = append(packages, filepath.Join(modulePath, pkg))
	}

	var cmd *exec.Cmd
	cmd = exec.Command("go", "test", "--tags=intest", "-p", strconv.Itoa(buildParallel), "--exec", xprogPath, "-vet", "off", "-count", "0")
	if coverprofile != "" {
		cmd.Args = append(cmd.Args, "-cover")
	}
	if race {
		cmd.Args = append(cmd.Args, "-race")
	}
	if short {
		cmd.Args = append(cmd.Args, "--test.short")
	}
	cmd.Args = append(cmd.Args, packages...)
	cmd.Dir = workDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return withTrace(err)
	}

	return nil
}

func testBinaryExist(pkg string) (bool, error) {
	_, err := os.Stat(testFileFullPath(pkg))
	if err != nil {
		//lint:ignore S1020
		if _, ok := err.(*os.PathError); ok {
			return false, nil
		}
	}
	return true, withTrace(err)
}

func testFileName(pkg string) string {
	_, file := filepath.Split(pkg)
	return file + ".test.bin"
}

func testFileFullPath(pkg string) string {
	return filepath.Join(workDir, pkg, testFileName(pkg))
}

func listNewTestCases(pkg string) ([]string, error) {
	exe := strings.Join([]string{".", testFileName(pkg)}, string(filepath.Separator))

	// session.test -test.list Test
	cmd := exec.Command(exe, "-test.list", "Test")
	cmd.Dir = filepath.Join(workDir, pkg)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	err := cmd.Run()
	res := strings.Split(buf.String(), "\n")
	if err != nil && len(res) == 0 {
		fmt.Println("err ==", err)
	}
	return filter(res, func(s string) bool {
		return strings.HasPrefix(s, "Test") && s != "TestT" && s != "TestBenchDaily"
	}), nil
}

func cmdToLines(cmd *exec.Cmd) ([]string, error) {
	res, err := cmd.Output()
	if err != nil {
		return nil, withTrace(err)
	}
	ss := bytes.Split(res, []byte{'\n'})
	ret := make([]string, len(ss))
	for i, s := range ss {
		ret[i] = string(s)
	}
	return ret, nil
}

func filter(input []string, f func(string) bool) []string {
	ret := input[:0]
	for _, s := range input {
		if f(s) {
			ret = append(ret, s)
		}
	}
	return ret
}

func shuffle(tasks []task) {
	for i := range tasks {
		pos := rand.Intn(len(tasks))
		tasks[i], tasks[pos] = tasks[pos], tasks[i]
	}
}

type errWithStack struct {
	err error
	buf []byte
}

func (e *errWithStack) Error() string {
	return e.err.Error() + "\n" + string(e.buf)
}

func withTrace(err error) error {
	if err == nil {
		return err
	}
	if _, ok := err.(*errWithStack); ok {
		return err
	}
	var stack [4096]byte
	sz := runtime.Stack(stack[:], false)
	return &errWithStack{err, stack[:sz]}
}

func formatDurationAsSeconds(d time.Duration) string {
	return fmt.Sprintf("%f", d.Seconds())
}

func packageProperties(goVersion string) []JUnitProperty {
	return []JUnitProperty{
		{Name: "go.version", Value: goVersion},
	}
}

// goVersion returns the version as reported by the go binary in PATH. This
// version will not be the same as runtime.Version, which is always the version
// of go used to build the gotestsum binary.
//
// To skip the os/exec call set the GOVERSION environment variable to the
// desired value.
func goVersion() string {
	if version, ok := os.LookupEnv("GOVERSION"); ok {
		return version
	}
	cmd := exec.Command("go", "version")
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimPrefix(strings.TrimSpace(string(out)), "go version ")
}

func write(out io.Writer, suites JUnitTestSuites) error {
	doc, err := xml.MarshalIndent(suites, "", "\t")
	if err != nil {
		return err
	}
	_, err = out.Write([]byte(xml.Header))
	if err != nil {
		return err
	}
	_, err = out.Write(doc)
	return err
}

// JUnitTestSuites is a collection of JUnit test suites.
type JUnitTestSuites struct {
	XMLName xml.Name `xml:"testsuites"`
	Suites  []JUnitTestSuite
}

// JUnitTestSuite is a single JUnit test suite which may contain many
// testcases.
type JUnitTestSuite struct {
	XMLName    xml.Name        `xml:"testsuite"`
	Tests      int             `xml:"tests,attr"`
	Failures   int             `xml:"failures,attr"`
	Time       string          `xml:"time,attr"`
	Name       string          `xml:"name,attr"`
	Properties []JUnitProperty `xml:"properties>property,omitempty"`
	TestCases  []JUnitTestCase
}

// JUnitTestCase is a single test case with its result.
type JUnitTestCase struct {
	XMLName     xml.Name          `xml:"testcase"`
	Classname   string            `xml:"classname,attr"`
	Name        string            `xml:"name,attr"`
	Time        string            `xml:"time,attr"`
	SkipMessage *JUnitSkipMessage `xml:"skipped,omitempty"`
	Failure     *JUnitFailure     `xml:"failure,omitempty"`
}

// JUnitSkipMessage contains the reason why a testcase was skipped.
type JUnitSkipMessage struct {
	Message string `xml:"message,attr"`
}

// JUnitProperty represents a key/value pair used to define properties.
type JUnitProperty struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

// JUnitFailure contains data related to a failed test.
type JUnitFailure struct {
	Message  string `xml:"message,attr"`
	Type     string `xml:"type,attr"`
	Contents string `xml:",chardata"`
}
