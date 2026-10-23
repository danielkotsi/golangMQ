package bench

import (
	"fmt"

	"github.com/danielkotsi/golangMQSDK/gomqSDK"
)

// ConnRole is a connection's workload role.
type ConnRole int

const (
	ConnMixed       ConnRole = iota
	ConnPublishOnly
	ConnConsumeOnly
)

func (r ConnRole) String() string {
	switch r {
	case ConnMixed:
		return "mixed"
	case ConnPublishOnly:
		return "pub"
	case ConnConsumeOnly:
		return "cons"
	}
	return "unknown"
}

// Role is a channel's workload role.
type Role int

const (
	RoleBoth    Role = iota
	RolePublish Role = iota
	RoleConsume
)

func (r Role) String() string {
	switch r {
	case RoleBoth:
		return "both"
	case RolePublish:
		return "publish"
	case RoleConsume:
		return "consume"
	}
	return "unknown"
}

// key groups a channel into the delivery-latency decomposition families
// defined by the plan: both vs consume-dedicated. Publish-only channels never
// carry deliveries, so they are excluded.
func (r Role) key() string {
	switch r {
	case RoleBoth:
		return "both"
	case RoleConsume:
		return "cons"
	}
	return ""
}

// channelSlot is one channel in the plan: its connection, its position within
// the connection, its role, and the goroutines placed on it. placement order
// within connPlans is the authoritative iteration order everywhere.
type channelSlot struct {
	Conn        int
	Ch          int          // index within the connection (0-based open order)
	GlobalIndex int          // order assigned during channel->conn placement
	Role        Role
	PubGoroutines  []int     // publisher goroutine indices placed here
	ConsGoroutines []int     // consumer goroutine indices placed here

	publishCapIdx int // index into publishCapable, -1 when not publish-capable
	client        *gomqSDK.ClientChannel
}

// connPlan is one connection and the channels hosted on it.
type connPlan struct {
	Conn     int
	Role     ConnRole
	Channels []*channelSlot
}

// Topology is the fully resolved benchmark plan.
type Topology struct {
	Conns           []connPlan
	publishCapable  []*channelSlot
	consumeCapable  []*channelSlot
}

// PublishCapable returns the publish-capable channels (P∪B) in plan order.
func (t *Topology) PublishCapable() []*channelSlot { return t.publishCapable }

// ConsumeCapable returns the consume-capable channels (C∪B) in plan order.
func (t *Topology) ConsumeCapable() []*channelSlot { return t.consumeCapable }

// Build resolves a Config into a deterministic Topology. It runs Validate
// first so config errors surface identically either way.
func Build(c *Config) (*Topology, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return buildTopology(c, true)
}

// buildTopology performs channel->conn placement, role resolution and
// goroutine placement. When assignWorkers is false only placement + roles are
// produced (used by Validate for overflow checks).
func buildTopology(c *Config, assignWorkers bool) (*Topology, error) {
	n := c.Conns

	// Step 1 — channel->connection placement.
	chans := make([][]int, n)
	switch c.ConnsDist {
	case DistEven:
		for i := 0; i < c.Channels; i++ {
			chans[i%n] = append(chans[i%n], i)
		}
	case DistUneven:
		capN := c.MaxChannelsPerConn
		if capN <= 0 {
			chans[0] = make([]int, c.Channels)
			for i := range chans[0] {
				chans[0][i] = i
			}
		} else {
			if c.Channels > n*capN {
				return nil, fmt.Errorf("uneven conns distribution: %d channels do not fit in %d conns x %d max-channels-per-conn",
					c.Channels, n, capN)
			}
			for i := 0; i < c.Channels; i++ {
				chans[i/capN] = append(chans[i/capN], i)
			}
		}
	}

	// Connection roles: the first -pub-conns are publish-only, the next
	// -cons-conns are consume-only, the rest are mixed.
	roles := make([]ConnRole, n)
	for i := range roles {
		switch {
		case i < c.PubConns:
			roles[i] = ConnPublishOnly
		case i < c.PubConns+c.ConsConns:
			roles[i] = ConnConsumeOnly
		default:
			roles[i] = ConnMixed
		}
	}

	// Build slots; allSlots ends up in global index order for every placement
	// mode (even and packing both assign ascending indices per connection, and
	// connections are visited ascending).
	t := &Topology{}
	allSlots := make([]*channelSlot, 0, c.Channels)
	for cn := 0; cn < n; cn++ {
		cp := connPlan{Conn: cn, Role: roles[cn]}
		for j, gi := range chans[cn] {
			sl := &channelSlot{
				Conn:        cn,
				Ch:          j,
				GlobalIndex: gi,
				publishCapIdx: -1,
			}
			switch roles[cn] {
			case ConnPublishOnly:
				sl.Role = RolePublish
			case ConnConsumeOnly:
				sl.Role = RoleConsume
			}
			cp.Channels = append(cp.Channels, sl)
			allSlots = append(allSlots, sl)
		}
		t.Conns = append(t.Conns, cp)
	}

	// Role stamping on mixed conns only: walk mixed slots in global index
	// order; the first -pub-channels become P, the next -cons-channels C,
	// the rest BOTH.
	var mixed []*channelSlot
	for _, sl := range allSlots {
		if roles[sl.Conn] == ConnMixed {
			mixed = append(mixed, sl)
		}
	}
	if carve := c.PubChannels + c.ConsChannels; carve > len(mixed) {
		return nil, fmt.Errorf("role carving overflow: pub-channels + cons-channels = %d exceed mixed-conn channel slots = %d",
			carve, len(mixed))
	}
	pubLeft, consLeft := c.PubChannels, c.ConsChannels
	for _, sl := range mixed {
		switch {
		case pubLeft > 0:
			sl.Role = RolePublish
			pubLeft--
		case consLeft > 0:
			sl.Role = RoleConsume
			consLeft--
		default:
			sl.Role = RoleBoth
		}
	}

	// Capability lists. Pass 1 collects role-stamped channels; pass 2 appends
	// the BOTH channels so each list is ordered P/C then B (matches the plan's
	// worked example placement — slot2 in publish-capable is channel 3, not
	// channel 1).
	for ci := range t.Conns {
		for _, sl := range t.Conns[ci].Channels {
			switch sl.Role {
			case RolePublish:
				t.publishCapable = append(t.publishCapable, sl)
			case RoleConsume:
				t.consumeCapable = append(t.consumeCapable, sl)
			}
		}
	}
	for ci := range t.Conns {
		for _, sl := range t.Conns[ci].Channels {
			if sl.Role == RoleBoth {
				t.publishCapable = append(t.publishCapable, sl)
				t.consumeCapable = append(t.consumeCapable, sl)
			}
		}
	}
	for i, sl := range t.publishCapable {
		sl.publishCapIdx = i
	}

	if !assignWorkers {
		return t, nil
	}

	// Step 3 — consumer -> channel placement over consumeCapable.
	if c.Consumers > 0 && len(t.consumeCapable) == 0 {
		return nil, fmt.Errorf("no consume-capable channels but -consumers = %d", c.Consumers)
	}
	if err := placeWorkers(c.Consumers, t.consumeCapable, c.ChannelsDist, c.MaxConsumersPerChannel,
		"consumers", "consume-capable channels", "max-consumers-per-channel",
		func(sl *channelSlot, j int) { sl.ConsGoroutines = append(sl.ConsGoroutines, j) }); err != nil {
		return nil, err
	}

	// Step 4 — publisher -> channel placement over publishCapable.
	if c.Publishers > 0 && len(t.publishCapable) == 0 {
		return nil, fmt.Errorf("no publish-capable channels but -publishers = %d", c.Publishers)
	}
	if err := placeWorkers(c.Publishers, t.publishCapable, c.PubChannelsDist, c.MaxPublishersPerChannel,
		"publishers", "publish-capable channels", "max-publishers-per-channel",
		func(sl *channelSlot, j int) { sl.PubGoroutines = append(sl.PubGoroutines, j) }); err != nil {
		return nil, err
	}

	return t, nil
}

// placeWorkers spreads count goroutines across slots with the given mode.
// even = deterministic round-robin (j -> slot j % len); uneven, no cap = pack
// everything into slot 0; uneven with cap = pack, failing when it cannot fit.
func placeWorkers(count int, slots []*channelSlot, mode Distribution, capN int,
	noun, slotsDesc, capFlag string, assign func(*channelSlot, int)) error {

	if count == 0 {
		return nil
	}
	if len(slots) == 0 {
		return fmt.Errorf("no %s to place %d %s", slotsDesc, count, noun)
	}
	switch mode {
	case DistEven:
		for j := 0; j < count; j++ {
			assign(slots[j%len(slots)], j)
		}
	case DistUneven:
		if capN <= 0 {
			for j := 0; j < count; j++ {
				assign(slots[0], j)
			}
			return nil
		}
		if count > len(slots)*capN {
			return fmt.Errorf("%d %s do not fit in %d %s x %d %s", count, noun, len(slots), slotsDesc, capN, capFlag)
		}
		for j := 0; j < count; j++ {
			assign(slots[(j/capN)%len(slots)], j)
		}
	default:
		return fmt.Errorf("invalid distribution %q", mode)
	}
	return nil
}