package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/AyKrimino/kriminoDB/internal/store"
)

type Server interface {
	Start() error
}

type Config struct {
	Host string
	Port string
}

type server struct {
	config Config
	store store.DB
}

func NewServer(s store.DB, conf Config) Server {
	return &server{
		store: s,
		config: conf,
	}
}

func (s *server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.config.Host, s.config.Port))
	if err != nil {
		return err
	}
	log.Printf("[SERVER] Listening on %s:%s", s.config.Host, s.config.Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("TCP accept error: %s", err)
		}

		log.Printf("[CONNECTION] New client from %s", conn.RemoteAddr())

		go s.handleConn(conn)
	}
}

func (s *server) handleConn(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			conn.Close()
			return
		}

		cmd, args, err := s.parseCommands(msg)
		if err != nil {
			fmt.Printf("TCP parse error: %s\n", err)
		}

		switch cmd {
		case "SET", "set":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
			} else {
				s.store.Set(args[0], []byte(args[1]))
				conn.Write([]byte("+OK\r\n"))
			}
		case "GET", "get":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
			} else {
				val, found := s.store.Get(args[0])
				if found {
					conn.Write(fmt.Appendf(nil, "%s=%s\n", args[0], string(val)))
				}
			}
		default:
			conn.Write(fmt.Appendf(nil, "-ERR unknown command!\n"))
		}

		log.Printf("[COMMAND] %s %v", cmd, args)
	}
}

func (s *server) parseCommands(msg string) (string, []string, error) {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return "", nil, fmt.Errorf("empty command")
	}

	idx := strings.Index(msg, " ")
	if idx == -1 {
		return strings.ToUpper(msg), nil, nil
	}

	cmd := strings.ToUpper(msg[:idx])
	rest := strings.TrimSpace(msg[idx+1:])
	if rest == "" {
		return cmd, nil, nil
	}

	parts := strings.SplitN(rest, " ", 2)

	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	if len(parts) == 0 || parts[0] == "" {
		return "", nil, fmt.Errorf("missing key")
	}

	return cmd, parts, nil
}
