package main

import (
	"io"
	"testing"
)

func TestParse(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/brokerd")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	cfg, err := parse([]string{"consume", "-group", "billing"}, io.Discard)
	if err != nil || cfg.command != "consume" || cfg.group != "billing" || cfg.brokers != "kafka:9092" || cfg.prefix != "brokerd." {
		t.Errorf("parse: %+v, %v", cfg, err)
	}
	for _, args := range [][]string{nil, {"replay"}, {"publish", "-partitions", "x"}} {
		if _, err = parse(args, io.Discard); err == nil {
			t.Errorf("parse(%q) succeeded", args)
		}
	}
	t.Setenv("DATABASE_URL", "")
	if _, err = parse([]string{"publish"}, io.Discard); err == nil {
		t.Error("parse without DATABASE_URL succeeded")
	}
}
