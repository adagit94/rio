package server

type Router[I ID] func(clients Clients[I], msg *MetaMessage[I])

// Broadcaster broadcast's message to all subscribed clients, excluding the sender.
func Broadcaster[I ID](clients Clients[I], msg *MetaMessage[I]) {
	for _, client := range clients {
		if client == msg.Sender {
			continue
		}

		client.WriteBuff <- msg
	}
}

type ConnectionsMap[I ID] map[I][]I

// Mapper send's the message to receiver's of the sender, as configured in connections Map. Receivers must be subscribed at the moment of evaluation.
func Mapper[I ID, M ConnectionsMap[I]](connections M) Router[I] {
	return func(clients Clients[I], msg *MetaMessage[I]) {
		receivers, found := connections[msg.Sender.ID]

		if !found {
			return
		}

		for _, rID := range receivers {
			r, found := clients[rID]

			if !found {
				continue
			}

			r.WriteBuff <- msg
		}
	}
}
