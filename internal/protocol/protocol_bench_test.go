package protocol

import (
	"bufio"
	"bytes"
	"testing"
)

func BenchmarkEncodeRequestSet(b *testing.B) {
	req := Request{
		Command: CommandSet,
		Key:     "benchmark-key",
		Value:   []byte("benchmark-value"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeRequest(req); err != nil {
			b.Fatalf("EncodeRequest() error = %v", err)
		}
	}
}

func BenchmarkDecodeRequestSet(b *testing.B) {
	req := Request{
		Command: CommandSet,
		Key:     "benchmark-key",
		Value:   []byte("benchmark-value"),
	}
	payload, err := EncodeRequest(req)
	if err != nil {
		b.Fatalf("EncodeRequest() error = %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bufio.NewReader(bytes.NewReader(payload))
		if _, err := DecodeRequest(reader); err != nil {
			b.Fatalf("DecodeRequest() error = %v", err)
		}
	}
}

func BenchmarkEncodeResponseValue(b *testing.B) {
	resp := Response{
		Kind:  ResponseValue,
		Value: []byte("benchmark-value"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeResponse(resp); err != nil {
			b.Fatalf("EncodeResponse() error = %v", err)
		}
	}
}

func BenchmarkDecodeResponseValue(b *testing.B) {
	resp := Response{
		Kind:  ResponseValue,
		Value: []byte("benchmark-value"),
	}
	payload, err := EncodeResponse(resp)
	if err != nil {
		b.Fatalf("EncodeResponse() error = %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bufio.NewReader(bytes.NewReader(payload))
		if _, err := DecodeResponse(reader); err != nil {
			b.Fatalf("DecodeResponse() error = %v", err)
		}
	}
}
