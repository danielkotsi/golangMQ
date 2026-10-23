package bench

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Distribution is the placement mode for one of the three distribution axes.
type Distribution string

const (
	DistEven   Distribution = "even"
	DistUneven Distribution = "uneven"
)

// Config captures every benchmark knob. The three distribution axes are
// populated by the order-sensitive -distribution flag; see distributionFlag.
type Config struct {
	Addr string

	// Topology
	Conns        int
	PubConns     int
	ConsConns    int
	Channels     int
	PubChannels  int
	ConsChannels int

	// Distribution axes
	ConnsDist       Distribution // channel -> connection placement
	ChannelsDist    Distribution // consumer -> channel placement
	PubChannelsDist Distribution // publisher -> channel placement

	// Caps (only meaningful when their axis is uneven)
	MaxChannelsPerConn      int
	MaxConsumersPerChannel  int
	MaxPublishersPerChannel int

	// Workload
	Publishers int
	Consumers  int
	Msgs       int
	Size       int
	Ack        bool
	Prefetch   int

	// Run / measurement
	Duration     time.Duration
	Warmup       time.Duration
	Report       time.Duration
	DrainTimeout time.Duration
	Output       string
	Quiet        bool
}

// DefaultConfig returns a Config with the locked Phase 2 defaults. Output is
// left empty so the engine can substitute the runid-based default path.
func DefaultConfig() *Config {
	return &Config{
		Addr: "127.0.0.1:5672",

		Conns:    1,
		Channels: 1,

		ConnsDist:       DistEven,
		ChannelsDist:    DistEven,
		PubChannelsDist: DistEven,

		Publishers: 1,
		Consumers:  1,
		Msgs:       1000,
		Size:       128,
		Ack:        true,
		Prefetch:   10,

		Report:       time.Second,
		DrainTimeout: 30 * time.Second,
	}
}

// Validate checks the flags and every placement-dependent overflow condition
// (role carving, cap packing). Placement-dependent checks delegate to the
// topology builder, so Validate and Build can never disagree.
func (c *Config) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("-addr must not be empty")
	}
	if c.Conns <= 0 {
		return fmt.Errorf("-conns must be >= 1 (got %d)", c.Conns)
	}
	if c.PubConns < 0 {
		return fmt.Errorf("-pub-conns must be >= 0 (got %d)", c.PubConns)
	}
	if c.ConsConns < 0 {
		return fmt.Errorf("-cons-conns must be >= 0 (got %d)", c.ConsConns)
	}
	if c.PubConns+c.ConsConns > c.Conns {
		return fmt.Errorf("pub-conns + cons-conns = %d exceeds conns = %d", c.PubConns+c.ConsConns, c.Conns)
	}
	if c.Channels <= 0 {
		return fmt.Errorf("-channels must be >= 1 (got %d)", c.Channels)
	}
	if c.PubChannels < 0 {
		return fmt.Errorf("-pub-channels must be >= 0 (got %d)", c.PubChannels)
	}
	if c.ConsChannels < 0 {
		return fmt.Errorf("-cons-channels must be >= 0 (got %d)", c.ConsChannels)
	}
	if c.Publishers < 0 {
		return fmt.Errorf("-publishers must be >= 0 (got %d)", c.Publishers)
	}
	if c.Consumers < 0 {
		return fmt.Errorf("-consumers must be >= 0 (got %d)", c.Consumers)
	}
	if c.Msgs < 0 {
		return fmt.Errorf("-msgs must be >= 0 (got %d)", c.Msgs)
	}
	if c.Size < headerLen {
		return fmt.Errorf("-size must be >= %d for the message identity header (got %d)", headerLen, c.Size)
	}
	if c.Report <= 0 {
		return fmt.Errorf("-report must be > 0 (got %v)", c.Report)
	}
	if c.DrainTimeout <= 0 {
		return fmt.Errorf("-drain-timeout must be > 0 (got %v)", c.DrainTimeout)
	}
	if c.Warmup < 0 {
		return fmt.Errorf("-warmup must be >= 0 (got %v)", c.Warmup)
	}
	if c.Duration < 0 {
		return fmt.Errorf("-duration must be >= 0 (got %v)", c.Duration)
	}
	if err := c.validateDistribution(); err != nil {
		return err
	}
	if _, err := buildTopology(c, true); err != nil {
		return err
	}
	return nil
}

func (c *Config) validateDistribution() error {
	switch c.ConnsDist {
	case DistEven, DistUneven:
	default:
		return fmt.Errorf("invalid conns distribution %q (want even|uneven)", c.ConnsDist)
	}
	switch c.ChannelsDist {
	case DistEven, DistUneven:
	default:
		return fmt.Errorf("invalid channels distribution %q (want even|uneven)", c.ChannelsDist)
	}
	switch c.PubChannelsDist {
	case DistEven, DistUneven:
	default:
		return fmt.Errorf("invalid pub-channels distribution %q (want even|uneven)", c.PubChannelsDist)
	}
	return nil
}

// Warnings returns advisory messages that do not invalidate the config.
func (c *Config) Warnings() []string {
	var w []string

	if total := c.Publishers * c.Msgs; total > 65535 {
		w = append(w, fmt.Sprintf(
			"publishers x msgs = %d exceeds the uint16 delivery-tag space (65535): delivery tags may wrap and dispatch can corrupt (Phase 3 deliberately probes this)",
			total))
	}
	if !c.Ack {
		w = append(w, "consumers will not ack: dispatch freezes each consumer at the broker's fixed prefetch (10); this run is a stall probe, not a throughput run")
	}
	if c.Prefetch != 10 {
		w = append(w, fmt.Sprintf("-prefetch %d ignored: the broker hardcodes a prefetch of 10 (no basic.qos)", c.Prefetch))
	}
	if c.MaxChannelsPerConn > 0 && c.ConnsDist != DistUneven {
		w = append(w, "-max-channels-per-conn ignored: the conns distribution axis is even (a cap only applies under uneven packing)")
	}
	if c.MaxConsumersPerChannel > 0 && c.ChannelsDist != DistUneven {
		w = append(w, "-max-consumers-per-channel ignored: the channels distribution axis is even (a cap only applies under uneven packing)")
	}
	if c.MaxPublishersPerChannel > 0 && c.PubChannelsDist != DistUneven {
		w = append(w, "-max-publishers-per-channel ignored: the pub-channels distribution axis is even (a cap only applies under uneven packing)")
	}
	return w
}

// AxisCursor remembers the last distribution-axis flag parsed (in argv order)
// so the order-sensitive -distribution flag can bind to it.
type AxisCursor struct {
	last string
}

// NewAxisInt returns a flag.Value that sets target and records its axis name
// in cur for the order-sensitive -distribution binding.
func NewAxisInt(target *int, axis string, cur *AxisCursor) flag.Value {
	return &intAxisFlag{val: target, axis: axis, cur: cur}
}

// NewDistributionFlag returns the order-sensitive -distribution flag.Value.
// Each occurrence reads AxisCursor.last (set by the preceding -conns,
// -channels or -pub-channels) and stores the mode on the matching axis.
func NewDistributionFlag(cfg *Config, cur *AxisCursor) flag.Value {
	return &distributionFlag{cfg: cfg, cur: cur}
}

type intAxisFlag struct {
	val  *int
	axis string
	cur  *AxisCursor
}

func (f *intAxisFlag) String() string { return strconv.Itoa(*f.val) }

func (f *intAxisFlag) Set(s string) error {
	v, err := strconv.Atoi(s)
	if err != nil {
		return fmt.Errorf("invalid integer %q", s)
	}
	*f.val = v
	f.cur.last = f.axis
	return nil
}

type distributionFlag struct {
	cfg *Config
	cur *AxisCursor
}

func (f *distributionFlag) String() string { return string(f.cfg.ConnsDist) }

func (f *distributionFlag) Set(s string) error {
	var d Distribution
	switch strings.ToLower(s) {
	case string(DistEven):
		d = DistEven
	case string(DistUneven):
		d = DistUneven
	default:
		return fmt.Errorf("invalid distribution %q (want even|uneven)", s)
	}
	switch f.cur.last {
	case "conns":
		f.cfg.ConnsDist = d
	case "channels":
		f.cfg.ChannelsDist = d
	case "pub-channels":
		f.cfg.PubChannelsDist = d
	default:
		return fmt.Errorf("-distribution must be preceded by -conns, -channels or -pub-channels on the command line")
	}
	return nil
}