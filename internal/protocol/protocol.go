package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

const (
	CommandSet            = "SET"
	CommandGet            = "GET"
	CommandDelete         = "DEL"
	CommandOwner          = "OWNER"
	CommandClusterMembers = "CLUSTER_MEMBERS"
	CommandClusterJoin    = "CLUSTER_JOIN"
	CommandClusterLeave   = "CLUSTER_LEAVE"
	CommandReplSet        = "REPL_SET"
	CommandReplDelete     = "REPL_DEL"
	CommandReplGet        = "REPL_GET"
	CommandPing           = "PING"
	CommandMetrics        = "METRICS"
	CommandRaftRequestVote   = "RAFT_REQUEST_VOTE"
	CommandRaftAppendEntries = "RAFT_APPEND_ENTRIES"
	CommandRaftStatus        = "RAFT_STATUS"
)

var (
	ErrInvalidRequest = errors.New("invalid request")
	ErrUnknownCommand = errors.New("unknown command")
)

// Request represents a parsed cache command.
type Request struct {
	Command string
	Key     string
	Value   []byte
	TTL     time.Duration
}

// ResponseKind identifies the response type sent to clients.
type ResponseKind int

const (
	ResponseOK ResponseKind = iota
	ResponseValue
	ResponseNotFound
	ResponseError
	ResponseMoved
	ResponseOwner
	ResponseMembers
	ResponseNotLeader
)

// Member describes a node in a cluster members response.
type Member struct {
	ID   string
	Addr string
}

// Response is a server reply to a client request.
type Response struct {
	Kind    ResponseKind
	Value   []byte
	Message string
	NodeID  string
	Addr    string
	Members []Member
}

// EncodeRequest serializes a request using a simple length-prefixed text format.
//
// Format:
//
//	CMD <command>
//	KEY <key>
//	VAL <length>
//	<value bytes>
//	TTL <seconds>   (optional, SET only)
//	END
func EncodeRequest(req Request) ([]byte, error) {
	if req.Command == "" {
		return nil, fmt.Errorf("%w: empty command", ErrInvalidRequest)
	}
	if req.Key == "" && req.Command != CommandClusterMembers && req.Command != CommandPing && req.Command != CommandMetrics && req.Command != CommandRaftStatus {
		return nil, fmt.Errorf("%w: empty key", ErrInvalidRequest)
	}
	if (req.Command == CommandSet || req.Command == CommandReplSet) && req.Value == nil {
		return nil, fmt.Errorf("%w: SET requires value", ErrInvalidRequest)
	}
	if (req.Command == CommandClusterJoin || req.Command == CommandRaftRequestVote || req.Command == CommandRaftAppendEntries) && req.Value == nil {
		return nil, fmt.Errorf("%w: command requires value", ErrInvalidRequest)
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CMD %s\n", req.Command)
	if req.Command != CommandClusterMembers && req.Command != CommandPing && req.Command != CommandMetrics && req.Command != CommandRaftStatus {
		fmt.Fprintf(&buf, "KEY %s\n", req.Key)
	}
	if req.Command == CommandSet || req.Command == CommandClusterJoin || req.Command == CommandReplSet ||
		req.Command == CommandRaftRequestVote || req.Command == CommandRaftAppendEntries {
		fmt.Fprintf(&buf, "VAL %d\n", len(req.Value))
		if _, err := buf.Write(req.Value); err != nil {
			return nil, err
		}
		buf.WriteByte('\n')
		if (req.Command == CommandSet || req.Command == CommandReplSet) && req.TTL > 0 {
			seconds := int64(req.TTL / time.Second)
			if seconds <= 0 {
				seconds = 1
			}
			fmt.Fprintf(&buf, "TTL %d\n", seconds)
		}
	}
	buf.WriteString("END\n")
	return buf.Bytes(), nil
}

// DecodeRequest parses one request from r.
func DecodeRequest(r *bufio.Reader) (Request, error) {
	var req Request

	cmdLine, err := readLine(r)
	if err != nil {
		return Request{}, err
	}
	if !strings.HasPrefix(cmdLine, "CMD ") {
		return Request{}, fmt.Errorf("%w: missing CMD line", ErrInvalidRequest)
	}
	req.Command = strings.TrimSpace(strings.TrimPrefix(cmdLine, "CMD "))
	if req.Command == "" {
		return Request{}, fmt.Errorf("%w: empty command", ErrInvalidRequest)
	}

	if req.Command == CommandClusterMembers || req.Command == CommandPing || req.Command == CommandMetrics || req.Command == CommandRaftStatus {
		endLine, err := readLine(r)
		if err != nil {
			return Request{}, err
		}
		if endLine != "END" {
			return Request{}, fmt.Errorf("%w: missing END line", ErrInvalidRequest)
		}
		return req, nil
	}

	keyLine, err := readLine(r)
	if err != nil {
		return Request{}, err
	}
	if !strings.HasPrefix(keyLine, "KEY ") {
		return Request{}, fmt.Errorf("%w: missing KEY line", ErrInvalidRequest)
	}
	req.Key = strings.TrimSpace(strings.TrimPrefix(keyLine, "KEY "))
	if req.Key == "" {
		return Request{}, fmt.Errorf("%w: empty key", ErrInvalidRequest)
	}

	switch req.Command {
	case CommandSet, CommandReplSet:
		return decodeSetRequest(r, req)
	case CommandClusterJoin, CommandRaftRequestVote, CommandRaftAppendEntries:
		return decodeValueRequest(r, req)
	case CommandGet, CommandDelete, CommandOwner, CommandClusterLeave, CommandReplDelete, CommandReplGet:
		endLine, err := readLine(r)
		if err != nil {
			return Request{}, err
		}
		if endLine != "END" {
			return Request{}, fmt.Errorf("%w: missing END line", ErrInvalidRequest)
		}
		return req, nil
	default:
		return Request{}, fmt.Errorf("%w: %s", ErrUnknownCommand, req.Command)
	}
}

func decodeSetRequest(r *bufio.Reader, req Request) (Request, error) {
	req, err := decodeValueBody(r, req)
	if err != nil {
		return Request{}, err
	}

	ttlLine, err := readLine(r)
	if err != nil {
		return Request{}, err
	}
	if strings.HasPrefix(ttlLine, "TTL ") {
		seconds, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(ttlLine, "TTL ")), 10, 64)
		if err != nil || seconds <= 0 {
			return Request{}, fmt.Errorf("%w: invalid TTL", ErrInvalidRequest)
		}
		req.TTL = time.Duration(seconds) * time.Second
		ttlLine, err = readLine(r)
		if err != nil {
			return Request{}, err
		}
	}
	if ttlLine != "END" {
		return Request{}, fmt.Errorf("%w: missing END line", ErrInvalidRequest)
	}
	return req, nil
}

func decodeValueRequest(r *bufio.Reader, req Request) (Request, error) {
	req, err := decodeValueBody(r, req)
	if err != nil {
		return Request{}, err
	}

	endLine, err := readLine(r)
	if err != nil {
		return Request{}, err
	}
	if endLine != "END" {
		return Request{}, fmt.Errorf("%w: missing END line", ErrInvalidRequest)
	}
	return req, nil
}

func decodeValueBody(r *bufio.Reader, req Request) (Request, error) {
	valLine, err := readLine(r)
	if err != nil {
		return Request{}, err
	}
	if !strings.HasPrefix(valLine, "VAL ") {
		return Request{}, fmt.Errorf("%w: missing VAL line", ErrInvalidRequest)
	}
	length, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(valLine, "VAL ")))
	if err != nil || length < 0 {
		return Request{}, fmt.Errorf("%w: invalid value length", ErrInvalidRequest)
	}
	value := make([]byte, length)
	if _, err := io.ReadFull(r, value); err != nil {
		return Request{}, err
	}
	if _, err := readLine(r); err != nil {
		return Request{}, err
	}
	req.Value = value
	return req, nil
}

// EncodeResponse serializes a response.
func EncodeResponse(resp Response) ([]byte, error) {
	var buf bytes.Buffer

	switch resp.Kind {
	case ResponseOK:
		buf.WriteString("OK\n")
	case ResponseValue:
		fmt.Fprintf(&buf, "VAL %d\n", len(resp.Value))
		if _, err := buf.Write(resp.Value); err != nil {
			return nil, err
		}
		buf.WriteByte('\n')
	case ResponseNotFound:
		buf.WriteString("NIL\n")
	case ResponseError:
		if resp.Message == "" {
			resp.Message = "error"
		}
		fmt.Fprintf(&buf, "ERR %s\n", resp.Message)
	case ResponseMoved:
		fmt.Fprintf(&buf, "MOVED %s %s\n", resp.NodeID, resp.Addr)
	case ResponseOwner:
		fmt.Fprintf(&buf, "OWNER %s %s\n", resp.NodeID, resp.Addr)
	case ResponseMembers:
		fmt.Fprintf(&buf, "MEMBERS %d\n", len(resp.Members))
		for _, member := range resp.Members {
			fmt.Fprintf(&buf, "%s %s\n", member.ID, member.Addr)
		}
	case ResponseNotLeader:
		fmt.Fprintf(&buf, "NOT_LEADER %s %s\n", resp.NodeID, resp.Addr)
	default:
		return nil, fmt.Errorf("%w: unknown response kind", ErrInvalidRequest)
	}

	buf.WriteString("END\n")
	return buf.Bytes(), nil
}

// DecodeResponse parses one response from r.
func DecodeResponse(r *bufio.Reader) (Response, error) {
	line, err := readLine(r)
	if err != nil {
		return Response{}, err
	}

	var resp Response
	switch {
	case line == "OK":
		resp.Kind = ResponseOK
	case line == "NIL":
		resp.Kind = ResponseNotFound
	case strings.HasPrefix(line, "ERR "):
		resp.Kind = ResponseError
		resp.Message = strings.TrimSpace(strings.TrimPrefix(line, "ERR "))
	case strings.HasPrefix(line, "MOVED "):
		resp.Kind = ResponseMoved
		if err := decodeNodeLine(line, "MOVED ", &resp.NodeID, &resp.Addr); err != nil {
			return Response{}, err
		}
	case strings.HasPrefix(line, "OWNER "):
		resp.Kind = ResponseOwner
		if err := decodeNodeLine(line, "OWNER ", &resp.NodeID, &resp.Addr); err != nil {
			return Response{}, err
		}
	case strings.HasPrefix(line, "MEMBERS "):
		resp.Kind = ResponseMembers
		count, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "MEMBERS ")))
		if err != nil || count < 0 {
			return Response{}, fmt.Errorf("%w: invalid members count", ErrInvalidRequest)
		}
		resp.Members = make([]Member, 0, count)
		for i := 0; i < count; i++ {
			memberLine, err := readLine(r)
			if err != nil {
				return Response{}, err
			}
			id, addr, ok := strings.Cut(memberLine, " ")
			if !ok || id == "" || addr == "" {
				return Response{}, fmt.Errorf("%w: invalid member line", ErrInvalidRequest)
			}
			resp.Members = append(resp.Members, Member{ID: id, Addr: addr})
		}
	case strings.HasPrefix(line, "NOT_LEADER "):
		resp.Kind = ResponseNotLeader
		if err := decodeNodeLine(line, "NOT_LEADER ", &resp.NodeID, &resp.Addr); err != nil {
			return Response{}, err
		}
	case strings.HasPrefix(line, "VAL "):
		resp.Kind = ResponseValue
		length, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "VAL ")))
		if err != nil || length < 0 {
			return Response{}, fmt.Errorf("%w: invalid value length", ErrInvalidRequest)
		}
		value := make([]byte, length)
		if _, err := io.ReadFull(r, value); err != nil {
			return Response{}, err
		}
		if _, err := readLine(r); err != nil {
			return Response{}, err
		}
		resp.Value = value
	default:
		return Response{}, fmt.Errorf("%w: unknown response line %q", ErrInvalidRequest, line)
	}

	endLine, err := readLine(r)
	if err != nil {
		return Response{}, err
	}
	if endLine != "END" {
		return Response{}, fmt.Errorf("%w: missing END line", ErrInvalidRequest)
	}

	return resp, nil
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	return line, nil
}

func decodeNodeLine(line, prefix string, nodeID, addr *string) error {
	parts := strings.Fields(strings.TrimPrefix(line, prefix))
	if len(parts) != 2 {
		return fmt.Errorf("%w: invalid node line %q", ErrInvalidRequest, line)
	}
	*nodeID = parts[0]
	*addr = parts[1]
	return nil
}
