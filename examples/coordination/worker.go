// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

// Command worker is a Go-SDK worker for the Actae coordination demo.
//
// It joins the same durable consumer group as the Python workers and claims
// the same invoices, proving that heterogeneous SDKs coordinate on one group.
// It runs in drain mode only (the crash is performed by the Python worker).
//
// Ledger receipts land on the audit channel, keeping the group's work channel
// free of non-work events so claim/ack/redelivery stay exact.
//
// Emitted lines use the same KEY<TAB>JSON protocol as worker.py:
// READY / JOINED / CLAIMED / SIDE_EFFECT / ACKED / DRAINED.
//
// Environment:
//
//	ACTAE_URL       HTTP base URL (default http://localhost:8002)
//	ACTAE_API_KEY   API key (default sk-dev-0000000000000000000000)
//
// Usage:
//
//	worker <channel> <group> <consumer_id> [--audit-channel CH] [--lease N]
//	       [--batch-limit N] [--out FILE]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	actae "github.com/divigaactae/actae/sdks/go"
)

const workType = "invoice.created"

func emit(out string, key string, data map[string]any) {
	line, _ := json.Marshal(data)
	msg := fmt.Sprintf("%s\t%s", key, line)
	if out != "" {
		f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			fmt.Fprintln(f, msg)
			f.Close()
			return
		}
	}
	fmt.Println(msg)
}

func sideEffect(invoiceID, amount any) map[string]any {
	time.Sleep(50 * time.Millisecond)
	return map[string]any{
		"processed":  true,
		"invoice_id": invoiceID,
		"amount":     amount,
		"ts":         time.Now().Unix(),
	}
}

func processInvoice(ctx context.Context, c *actae.Client, workChannel, auditChannel, group, consumerID string,
	ev actae.Event, out string) {
	payload, _ := ev.Payload.(map[string]any)
	invoiceID := payload["invoice_id"]
	amount := payload["amount"]

	claim, err := c.ClaimExecution(ctx, auditChannel,
		fmt.Sprintf("invoice:%v", invoiceID), "process_invoice",
		map[string]any{"invoice_id": invoiceID, "amount": amount},
		actae.ClaimExecutionOptions{EmitReplayEvent: true, LeaseSeconds: actae.Ptr(60)})
	if err != nil {
		emit(out, "SIDE_EFFECT_ERROR", map[string]any{"invoice_id": invoiceID, "error": err.Error()})
		return
	}
	switch claim.Status {
	case "replayed":
		emit(out, "SIDE_EFFECT", map[string]any{
			"invoice_id": invoiceID, "cursor": ev.Cursor, "status": "replayed",
			"ran": false, "execution_id": claim.Execution.ID,
		})
		return
	case "in_progress":
		emit(out, "SIDE_EFFECT", map[string]any{
			"invoice_id": invoiceID, "cursor": ev.Cursor, "status": "in_progress", "ran": false,
		})
		return
	}

	result := sideEffect(invoiceID, amount)
	if _, err := c.CompleteExecution(ctx, claim.Execution.ID, claim.ClaimToken, result); err != nil {
		emit(out, "SIDE_EFFECT_ERROR", map[string]any{"invoice_id": invoiceID, "error": err.Error()})
		return
	}
	emit(out, "SIDE_EFFECT", map[string]any{
		"invoice_id": invoiceID, "cursor": ev.Cursor, "status": claim.Status,
		"ran": true, "execution_id": claim.Execution.ID,
	})
}

func main() {
	lease := flag.Int("lease", 8, "lease seconds")
	limit := flag.Int("batch-limit", 8, "claim limit")
	emptyGrace := flag.Float64("empty-grace", 5.0, "keep retrying empty claims for this many seconds before draining")
	auditChannel := flag.String("audit-channel", "", "channel for ledger receipts + worker lifecycle (default: work channel)")
	out := flag.String("out", "", "append KEY<TAB>JSON lines to this file")
	flag.Parse()
	if flag.NArg() != 3 {
		fmt.Fprintln(os.Stderr, "usage: worker <channel> <group> <consumer_id> [--audit-channel CH] [--lease N] [--batch-limit N] [--empty-grace S] [--out FILE]")
		os.Exit(2)
	}
	channel, group, consumerID := flag.Arg(0), flag.Arg(1), flag.Arg(2)
	if *auditChannel == "" {
		*auditChannel = channel
	}

	ctx := context.Background()
	opts := actae.ClientOptions{}
	if url := os.Getenv("ACTAE_URL"); url != "" {
		opts.Endpoint = url
	} else {
		opts.Endpoint = "http://localhost:8002"
	}
	if key := os.Getenv("ACTAE_API_KEY"); key != "" {
		opts.APIKey = key
	} else {
		opts.APIKey = "sk-dev-0000000000000000000000"
	}
	client, err := actae.NewClientFromEnv(opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}

	if _, err := client.Record(ctx, *auditChannel, "worker.started",
		map[string]any{"consumer": consumerID, "kind": "go"}, actae.RecordOptions{Actor: consumerID}); err != nil {
		fmt.Fprintln(os.Stderr, "worker: record:", err)
		os.Exit(1)
	}
	emit(*out, "READY", map[string]any{"consumer": consumerID})
	if _, err := client.JoinGroup(ctx, group, consumerID, *lease); err != nil {
		fmt.Fprintln(os.Stderr, "worker: join:", err)
		os.Exit(1)
	}
	emit(*out, "JOINED", map[string]any{"consumer": consumerID, "lease": *lease})

	var firstEmpty time.Time
	for {
		work, err := client.ClaimWork(ctx, group, consumerID, *limit)
		if err != nil {
			fmt.Fprintln(os.Stderr, "worker: claim:", err)
			os.Exit(1)
		}
		if len(work.Events) == 0 {
			if firstEmpty.IsZero() {
				firstEmpty = time.Now()
			} else if time.Since(firstEmpty) >= time.Duration(*emptyGrace*float64(time.Second)) {
				emit(*out, "DRAINED", map[string]any{"consumer": consumerID})
				return
			}
			time.Sleep(time.Second)
			continue
		}
		firstEmpty = time.Time{}
		var invoices []actae.Event
		for _, ev := range work.Events {
			if ev.EventType == workType {
				invoices = append(invoices, ev)
			}
		}
		for _, ev := range invoices {
			processInvoice(ctx, client, channel, *auditChannel, group, consumerID, ev, *out)
		}
		last := work.Events[len(work.Events)-1].Cursor
		if err := client.AckWork(ctx, group, consumerID, last); err != nil {
			fmt.Fprintln(os.Stderr, "worker: ack:", err)
			os.Exit(1)
		}
		emit(*out, "ACKED", map[string]any{"consumer": consumerID, "cursor": last, "invoices": len(invoices)})
	}
}
