package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"GolangRabbitMQBroker/broker"
	"GolangRabbitMQBroker/internal/bench"
)

func startBenchBroker(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := broker.NewServer(ln.Addr().String(), broker.ServerConfig{
		ChannelMax:   10,
		FramesMax:    10372,
		HeartbeatSec: 10,
	})
	t.Cleanup(func() { _ = ln.Close() })
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String()
}

// captureRun executes run() with a controlled argv and captures both streams.
// run() never calls os.Exit, so the test process is safe.
func captureRun(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()

	oldStdout, oldStderr, oldArgs := os.Stdout, os.Stderr, os.Args
	defer func() {
		os.Stdout = oldStdout
		os.Stderr = oldStderr
		os.Args = oldArgs
	}()

	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}

	var outBuf, errBuf bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = io.Copy(&outBuf, outR) }()
	go func() { defer wg.Done(); _, _ = io.Copy(&errBuf, errR) }()

	os.Stdout, os.Stderr = outW, errW
	os.Args = append([]string{"benchmark"}, args...)

	code = run()

	_ = outW.Close()
	_ = errW.Close()
	wg.Wait()
	_ = outR.Close()
	_ = errR.Close()
	return code, outBuf.String(), errBuf.String()
}

func TestCLI_ParseErrorsExitTwo(t *testing.T) {
	cases := [][]string{
		{"-distribution", "even"}, // no preceding axis
		{"-bogus"},
		{"-conns", "not-an-int"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			code, _, _ := captureRun(t, args...)
			if code != 2 {
				t.Fatalf("args=%v exit=%d want 2", args, code)
			}
		})
	}
}

func TestCLI_ValidationErrorsExitOne(t *testing.T) {
	cases := [][]string{
		{"-conns", "0"},
		{"-size", "10"},
		{"-conns", "1", "-pub-conns", "1", "-cons-conns", "1"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			code, _, _ := captureRun(t, args...)
			if code != 1 {
				t.Fatalf("args=%v exit=%d want 1", args, code)
			}
		})
	}
}

func TestCLI_UnreachableBrokerExitsOne(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	code, _, stderr := captureRun(t,
		"-addr", addr,
		"-output", filepath.Join(t.TempDir(), "run.csv"),
		"-msgs", "1",
		"-drain-timeout", "1s",
		"-quiet",
	)
	if code != 1 {
		t.Fatalf("exit=%d want 1 (stderr=%q)", code, stderr)
	}
}

func TestCLI_JSONSummaryOnStdout(t *testing.T) {
	addr := startBenchBroker(t)
	output := filepath.Join(t.TempDir(), "run.csv")

	code, stdout, stderr := captureRun(t,
		"-addr", addr,
		"-output", output,
		"-msgs", "20",
		"-report", "10ms",
		"-drain-timeout", "2s",
		"-quiet",
	)
	if code != 0 {
		t.Fatalf("exit=%d want 0 (stderr=%q)", code, stderr)
	}

	trimmed := strings.TrimSpace(stdout)
	if strings.Count(trimmed, "\n") != 0 {
		t.Fatalf("stdout should contain exactly one JSON line, got:\n%s", stdout)
	}
	var summary bench.Summary
	if err := json.Unmarshal([]byte(trimmed), &summary); err != nil {
		t.Fatalf("stdout is not a JSON summary: %v\nstdout=%q", err, stdout)
	}
	if summary.Published != 20 || summary.Consumed != 20 || summary.Drops != 0 {
		t.Fatalf("summary published/consumed/drops=%d/%d/%d want 20/20/0",
			summary.Published, summary.Consumed, summary.Drops)
	}
	if _, err := os.Stat(output); err != nil {
		t.Fatalf("CSV file was not written: %v", err)
	}
}

// TestFinding_Bench005_OutputDashMustNotMixStreams captures the documented
// stream-mixing pitfall: with -output -, the CSV document is written to stdout
// and then the JSON summary is appended to the same stdout. Consumers cannot
// safely pipe stdout as CSV or as JSON.
func TestFinding_Bench005_OutputDashMustNotMixStreams(t *testing.T) {
	addr := startBenchBroker(t)

	code, stdout, stderr := captureRun(t,
		"-addr", addr,
		"-output", "-",
		"-msgs", "10",
		"-report", "10ms",
		"-drain-timeout", "2s",
		"-quiet",
	)
	if code != 0 {
		t.Fatalf("exit=%d want 0 (stderr=%q)", code, stderr)
	}

	hasCSVHeader := strings.Contains(stdout, "ts_start_rfc3339,window_ms,published")
	hasJSON := strings.Contains(stdout, "\"runid\"")
	if hasCSVHeader && hasJSON {
		t.Fatalf("stdout mixes CSV and JSON in one stream; stdout=\n%s", stdout)
	}
}
