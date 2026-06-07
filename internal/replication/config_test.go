package replication

import "testing"

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{name: "disabled", cfg: Config{ReplicationFactor: 1}},
		{name: "valid", cfg: DefaultConfig()},
		{name: "bad write quorum", cfg: Config{ReplicationFactor: 3, WriteQuorum: 4, ReadQuorum: 2, WriteConsistency: WriteQuorum, ReadConsistency: ReadQuorum}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.cfg.Normalize().Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRequiredWriteAcks(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	if got := cfg.RequiredWriteAcks(); got != 1 {
		t.Fatalf("RequiredWriteAcks() = %d, want 1", got)
	}

	cfg.WriteConsistency = WriteAll
	if got := cfg.RequiredWriteAcks(); got != 2 {
		t.Fatalf("RequiredWriteAcks(all) = %d, want 2", got)
	}
}
