package status

/*
// Connection client the base infomation
type Connection struct {
	Op       string `json:"op"`
	Service  string `json:"service"`
	ClientID string `json:"cid"`
	Force    bool   `json:"force"`
}

// closeHandleFunc close the corresponding connection based on the business name and clientid
func (s *Server) closeHandleFunc(w http.ResponseWriter, r *http.Request) {
	var msg Connection
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&msg); err != nil {
		fmt.Fprintf(w, fmt.Sprintf("Request body must be well-formed JSON: %v\n", err))
		return
	}

	if msg.Op != "disconnect" {
		fmt.Fprintf(w, "the num of disconnect successful clients : %v\n", 0)
		return
	}

	if !msg.Force && len(msg.Service) == 0 && len(msg.ClientID) == 0 {
		fmt.Fprintf(w, "service is null,clientid is null\n")
		return
	}

	count := 0
	if len(msg.Service) != 0 && len(msg.ClientID) != 0 {
		if c := clients.GlobalClients().Get(msg.Service + msg.ClientID); c != nil {
			c.(*session.Session).Close()
			count++
		}
	} else {
		clients.GlobalClients().Scan(func(c interface{}) error {
			c.(*session.Session).Close()
			return nil
		})
	}
	fmt.Fprintf(w, "the num of disconnect successful clients : %v\n", count)
}
*/
