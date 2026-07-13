package server

func Broadcaster[I ID](clients Clients[I], msg *MessageIntern[I]) {
	for _, client := range clients {
		if client == msg.Source {
			continue
		}

		client.WriteBuff <- msg
	}
}

func Mapper[I ID, M map[I][]I](connections M) Router[I] {
	return func(clients Clients[I], msg *MessageIntern[I]) {
		receivers, found := connections[msg.Source.ID]

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
