package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"GolangRabbitMQBroker/internal/bench"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg := bench.DefaultConfig()
	var cur bench.AxisCursor

	fs := flag.NewFlagSet("benchmark", flag.ContinueOnError)
	fs.StringVar(&cfg.Addr, "addr", cfg.Addr, "broker host:port")

	fs.Var(bench.NewAxisInt(&cfg.Conns, "conns", &cur), "conns", "total connections (distribution axis)")
	fs.IntVar(&cfg.PubConns, "pub-conns", cfg.PubConns, "connections dedicated to publishing")
	fs.IntVar(&cfg.ConsConns, "cons-conns", cfg.ConsConns, "connections dedicated to consuming")
	fs.Var(bench.NewAxisInt(&cfg.Channels, "channels", &cur), "channels", "total channels across all connections (distribution axis)")
	fs.Var(bench.NewAxisInt(&cfg.PubChannels, "pub-channels", &cur), "pub-channels", "publish-only channel slots carved from mixed-conn slots (distribution axis)")
	fs.IntVar(&cfg.ConsChannels, "cons-channels", cfg.ConsChannels, "consume-only channel slots carved from mixed-conn slots")
	fs.Var(bench.NewDistributionFlag(cfg, &cur), "distribution", "placement mode even|uneven; binds to the -conns/-channels/-pub-channels that immediately precede it")

	fs.IntVar(&cfg.MaxChannelsPerConn, "max-channels-per-conn", cfg.MaxChannelsPerConn, "cap for the uneven conns distribution")
	fs.IntVar(&cfg.MaxConsumersPerChannel, "max-consumers-per-channel", cfg.MaxConsumersPerChannel, "cap for the uneven channels distribution")
	fs.IntVar(&cfg.MaxPublishersPerChannel, "max-publishers-per-channel", cfg.MaxPublishersPerChannel, "cap for the uneven pub-channels distribution")

	fs.IntVar(&cfg.Publishers, "publishers", cfg.Publishers, "publisher goroutines")
	fs.IntVar(&cfg.Consumers, "consumers", cfg.Consumers, "consumer goroutines")
	fs.IntVar(&cfg.Msgs, "msgs", cfg.Msgs, "messages per publisher (total = publishers x msgs)")
	fs.IntVar(&cfg.Size, "size", cfg.Size, "payload bytes (must be >= 16)")
	fs.BoolVar(&cfg.Ack, "ack", cfg.Ack, "consumers ack after each delivery")
	fs.IntVar(&cfg.Prefetch, "prefetch", cfg.Prefetch, "accepted for forward-compat only; broker hardcodes 10")

	fs.DurationVar(&cfg.Duration, "duration", cfg.Duration, "0 = run to completion; >0 = publishers stop at deadline, then drain")
	fs.DurationVar(&cfg.Warmup, "warmup", cfg.Warmup, "traffic counted but excluded from stats")
	fs.DurationVar(&cfg.Report, "report", cfg.Report, "sampler cadence (CSV rows)")
	fs.DurationVar(&cfg.DrainTimeout, "drain-timeout", cfg.DrainTimeout, "max wait for the queue to empty")
	fs.StringVar(&cfg.Output, "output", cfg.Output, "CSV path (default bench-<runid>.csv; '-' = stdout, JSON summary then goes to stderr)")
	fs.BoolVar(&cfg.Quiet, "quiet", cfg.Quiet, "suppress per-interval log lines")

	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: benchmark [options]\n\noptions:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		fmt.Fprintln(os.Stderr, err)
		fs.Usage()
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "benchmark: unexpected arguments: %v\n", fs.Args())
		fs.Usage()
		return 2
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "benchmark: %v\n", err)
		fs.Usage()
		return 1
	}
	for _, w := range cfg.Warnings() {
		fmt.Fprintf(os.Stderr, "benchmark: WARNING: %s\n", w)
	}

	res, err := bench.Run(context.Background(), cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchmark: %v\n", err)
		return 1
	}

	if !cfg.Quiet {
		fmt.Fprintf(os.Stderr, "benchmark: run %s complete: published=%d consumed=%d drops=%d elapsed=%dms csv=%s\n",
			res.RunID, res.Summary.Published, res.Summary.Consumed, res.Summary.Drops, res.Summary.ElapsedMs, res.CSVPath)
	}

	data, err := json.Marshal(res.Summary)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchmark: marshal summary: %v\n", err)
		return 1
	}
	if cfg.Output == "-" {
		fmt.Fprintln(os.Stderr, string(data))
	} else {
		fmt.Println(string(data))
	}
	return 0
}
