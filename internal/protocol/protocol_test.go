package protocol

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestEncodeDecodeRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		req  Request
	}{
		{
			name: "set",
			req: Request{
				Command: CommandSet,
				Key:     "user:1",
				Value:   []byte("alice"),
			},
		},
		{
			name: "set with ttl",
			req: Request{
				Command: CommandSet,
				Key:     "session",
				Value:   []byte("token"),
				TTL:     30 * time.Second,
			},
		},
		{
			name: "get",
			req: Request{
				Command: CommandGet,
				Key:     "user:1",
			},
		},
		{
			name: "delete",
			req: Request{
				Command: CommandDelete,
				Key:     "user:1",
			},
		},
		{
			name: "binary value",
			req: Request{
				Command: CommandSet,
				Key:     "blob",
				Value:   []byte{0x00, 0x01, '\n', '\r'},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encoded, err := EncodeRequest(tt.req)
			if err != nil {
				t.Fatalf("EncodeRequest() error = %v", err)
			}

			got, err := DecodeRequest(bufio.NewReader(bytes.NewReader(encoded)))
			if err != nil {
				t.Fatalf("DecodeRequest() error = %v", err)
			}

			if got.Command != tt.req.Command {
				t.Fatalf("command = %q, want %q", got.Command, tt.req.Command)
			}
			if got.Key != tt.req.Key {
				t.Fatalf("key = %q, want %q", got.Key, tt.req.Key)
			}
			if !bytes.Equal(got.Value, tt.req.Value) {
				t.Fatalf("value = %q, want %q", got.Value, tt.req.Value)
			}
			if got.TTL != tt.req.TTL {
				t.Fatalf("ttl = %v, want %v", got.TTL, tt.req.TTL)
			}
		})
	}
}

func TestEncodeDecodeResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		resp Response
	}{
		{name: "ok", resp: Response{Kind: ResponseOK}},
		{name: "nil", resp: Response{Kind: ResponseNotFound}},
		{name: "error", resp: Response{Kind: ResponseError, Message: "bad request"}},
		{name: "value", resp: Response{Kind: ResponseValue, Value: []byte("payload\n")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encoded, err := EncodeResponse(tt.resp)
			if err != nil {
				t.Fatalf("EncodeResponse() error = %v", err)
			}

			got, err := DecodeResponse(bufio.NewReader(bytes.NewReader(encoded)))
			if err != nil {
				t.Fatalf("DecodeResponse() error = %v", err)
			}

			if got.Kind != tt.resp.Kind {
				t.Fatalf("kind = %v, want %v", got.Kind, tt.resp.Kind)
			}
			if got.Message != tt.resp.Message {
				t.Fatalf("message = %q, want %q", got.Message, tt.resp.Message)
			}
			if !bytes.Equal(got.Value, tt.resp.Value) {
				t.Fatalf("value = %q, want %q", got.Value, tt.resp.Value)
			}
		})
	}
}

func TestEncodeDecodeResponseCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		resp Response
	}{
		{
			name: "moved",
			resp: Response{Kind: ResponseMoved, NodeID: "node-a", Addr: "127.0.0.1:6379"},
		},
		{
			name: "owner",
			resp: Response{Kind: ResponseOwner, NodeID: "node-b", Addr: "127.0.0.1:6380"},
		},
		{
			name: "members",
			resp: Response{
				Kind: ResponseMembers,
				Members: []Member{
					{ID: "node-a", Addr: "127.0.0.1:6379"},
					{ID: "node-b", Addr: "127.0.0.1:6380"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encoded, err := EncodeResponse(tt.resp)
			if err != nil {
				t.Fatalf("EncodeResponse() error = %v", err)
			}

			got, err := DecodeResponse(bufio.NewReader(bytes.NewReader(encoded)))
			if err != nil {
				t.Fatalf("DecodeResponse() error = %v", err)
			}
			if got.Kind != tt.resp.Kind {
				t.Fatalf("kind = %v, want %v", got.Kind, tt.resp.Kind)
			}
			if got.NodeID != tt.resp.NodeID || got.Addr != tt.resp.Addr {
				t.Fatalf("node = %q/%q, want %q/%q", got.NodeID, got.Addr, tt.resp.NodeID, tt.resp.Addr)
			}
			if len(got.Members) != len(tt.resp.Members) {
				t.Fatalf("members = %+v, want %+v", got.Members, tt.resp.Members)
			}
		})
	}
}

func TestEncodeDecodeClusterMembersRequest(t *testing.T) {
	t.Parallel()

	req := Request{Command: CommandClusterMembers}
	encoded, err := EncodeRequest(req)
	if err != nil {
		t.Fatalf("EncodeRequest() error = %v", err)
	}

	got, err := DecodeRequest(bufio.NewReader(bytes.NewReader(encoded)))
	if err != nil {
		t.Fatalf("DecodeRequest() error = %v", err)
	}
	if got.Command != CommandClusterMembers {
		t.Fatalf("command = %q", got.Command)
	}
}

func TestDecodeRequestInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
	}{
		{name: "missing end", payload: "CMD GET\nKEY foo\n"},
		{name: "unknown command", payload: "CMD PING\nKEY foo\nEND\n"},
		{name: "set missing value", payload: "CMD SET\nKEY foo\nEND\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := DecodeRequest(bufio.NewReader(strings.NewReader(tt.payload)))
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
