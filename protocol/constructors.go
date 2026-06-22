package protocol

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

func NewConnectionStartOK(clientname, username, password string) ConnectionStartOK {
	return ConnectionStartOK{
		Type:          TypeConnectionStartOK,
		ClientName:    clientname,
		AuthMechanism: AuthMechanismPlain,
		Username:      username,
		Password:      password,
	}
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

func NewConnectionOpen() ConnectionOpen {
	return ConnectionOpen{
		Type: TypeConnectionOpen,
	}
}

func NewConnectionOpenOK() ConnectionOpenOK {
	return ConnectionOpenOK{
		Type: TypeConnectionOpenOK,
	}
}
