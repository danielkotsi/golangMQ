package protocol

//for now until it is more clear to me the AuthMechanism is set to "plain" and it is hardcoded, might inspect later other options

const (
	ProtocolName          = "GOMQ"
	ProtocolVersion       = "1"
	ProtocolHeader        = ProtocolName + "/" + ProtocolVersion
	ServerName            = ProtocolName + "-Broker"
	TypeConnectionStart   = "connection.start"
	TypeConnectionStartOK = "connection.start_ok"
	TypeConnectionTune    = "connection.tune"
	TypeConnectionTuneOK  = "connection.tune_ok"
	TypeConnectionOpen    = "connection.open"
	TypeConnectionOpenOK  = "connection.open_ok"
	AuthMechanismPlain    = "plain"
)

type ConnectionStart struct {
	Type            string       `json:"type"`
	ServerName      string       `json:"server_name"`
	ProtocolVersion string       `json:"protocol_version"`
	AuthMechanism   string       `json:"auth_mechanism"`
	Capabilities    Capabilities `json:"capabilities,omitempty"`
}

type Capabilities struct {
	Heartbeats bool `json:"heartbeats,omitempty"`
}

func NewConnectionStart() ConnectionStart {
	return ConnectionStart{
		Type:            TypeConnectionStart,
		ServerName:      ServerName,
		ProtocolVersion: ProtocolVersion,
		AuthMechanism:   AuthMechanismPlain,
		Capabilities: Capabilities{
			Heartbeats: true,
		},
	}
}

type ConnectionStartOK struct {
	Type          string `json:"type"`
	ClientName    string `json:"client_name"`
	AuthMechanism string `json:"auth_mechanism"`
	Username      string `json:"username"`
	Password      string `json:"password"`
}

func NewConnectionStartOK(clientname, username, password string) ConnectionStartOK {
	return ConnectionStartOK{
		Type:          TypeConnectionStartOK,
		ClientName:    clientname,
		AuthMechanism: AuthMechanismPlain,
		Username:      username,
		Password:      password,
	}
}

type ConnectionTune struct {
	Type         string `json:"type"`
	ChannelMax   int    `json:"channel_max"`
	FrameMax     int    `json:"frame_max"`
	HeartbeatSec int    `json:"heartbeat_sec"`
}

func NewConnectionTune(
	channelMax int,
	frameMax int,
	heartbeatSec int,
) ConnectionTune {
	return ConnectionTune{
		Type:         TypeConnectionTune,
		ChannelMax:   channelMax,
		FrameMax:     frameMax,
		HeartbeatSec: heartbeatSec,
	}
}

type ConnectionTuneOK struct {
	Type         string `json:"type"`
	ChannelMax   int    `json:"channel_max"`
	FrameMax     int    `json:"frame_max"`
	HeartbeatSec int    `json:"heartbeat_sec"`
}

func NewConnectionTuneOK(
	channelMax int,
	frameMax int,
	heartbeatSec int,
) ConnectionTuneOK {
	return ConnectionTuneOK{
		Type:         TypeConnectionTuneOK,
		ChannelMax:   channelMax,
		FrameMax:     frameMax,
		HeartbeatSec: heartbeatSec,
	}
}
