package broker

type Router struct {
}

func NewRouter() *Router { return &Router{} }

// func (r *Router) Handle(conn *Connection, env protocol.Envelope) error {
// 	switch env.Type {
// 	case protocol.BasicPublishType:
// 		var event protocol.Publish
// 		err := json.Unmarshal(env.Payload, &event)
// 		if err != nil {
// 			return err
// 		}
// 		return conn.server.HandlePublish(conn, &event)
// 	case protocol.BasicConsumeType:
// 		var event protocol.Consume
// 		err := json.Unmarshal(env.Payload, &event)
// 		if err != nil {
// 			return err
// 		}
// 		return conn.server.HandleConsume(conn, &event)
// 	case protocol.BasicAckType:
// 		var event protocol.Ack
// 		err := json.Unmarshal(env.Payload, &event)
// 		if err != nil {
// 			return err
// 		}
// 		return conn.server.HandleAck(conn, &event)
// 	case protocol.ChannelOpenType:
// 		var event protocol.ChannelOpen
// 		err := json.Unmarshal(env.Payload, &event)
// 		if err != nil {
// 			return err
// 		}
// 		return conn.server.HandleChannelOpen(conn, &event)
// 	case protocol.QueueDeclareType:
// 		var event protocol.QueueDeclare
// 		err := json.Unmarshal(env.Payload, &event)
// 		if err != nil {
// 			return err
// 		}
// 		return conn.server.HandleQueueDeclare(conn, &event)
// 	case protocol.QueueBindType:
// 		var event protocol.QueueBind
// 		err := json.Unmarshal(env.Payload, &event)
// 		if err != nil {
// 			return err
// 		}
// 		return conn.server.HandleQueueBind(conn, &event)
// 	}
// 	return nil
// }
