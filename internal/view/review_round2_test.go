package view

// The reviewer's probes of the second fix round, kept as they were written where the head keeps their expectation.
// TestRound2MuxBeforeGuard failed on 86d0b3d and passes at the head. TestRound2LateEnd and TestRound2RegrantGap are
// inverted: since round 3 membership and entitlement over time are the ACLs' decision, so the records of both
// scenarios display, labelled with the receiver whose key verified them.

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
)

func TestRound2MuxBeforeGuard(t *testing.T) {
	s := newServed(t, ServeOptions{})
	response, body := s.request(t, "GET", "/s3cr3t-path//tail?token=s3cr3t-query", nil, nil, map[string]string{"Host": "foreign.invalid", "Origin": "https://foreign.invalid"})
	t.Logf("status=%d location=%q cache=%q csp=%q body=%q", response.StatusCode, response.Header.Get("Location"), response.Header.Get("Cache-Control"), response.Header.Get("Content-Security-Policy"), body)
	if response.StatusCode != http.StatusForbidden || strings.Contains(body+response.Header.Get("Location"), "s3cr3t") {
		t.Fatal("mux redirect bypasses guard and reflects request text")
	}
}

func TestRound2LateEnd(t *testing.T) {
	for _, member := range []bool{false, true} {
		t.Run(map[bool]string{false: "entitlement", true: "member"}[member], func(t *testing.T) {
			f := newFixture(t)
			at := f.now.Add(-time.Minute)
			run := f.echo(at, receiverA, tenantA, hostA, poolA, "late-end")
			f.status(at.Add(3*time.Second), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateCompleted})
			v := f.view(time.Hour, testCaps())
			// The operator end is committed after the operator read but before the data read in a tick.
			// Backdate its timestamp to represent that interleaving, as the retirement probes do.
			if member {
				f.operator(at.Add(time.Second), operator.Record{Kind: "end", Receiver: receiverB})
			} else {
				// The echo too was appended after the entitlement ended.
				f.operator(at.Add(-time.Second), operator.Record{Kind: "end", Tenant: tenantA, Pool: poolA})
			}
			f.tick(v)
			d, ok := v.RunDetail(run)
			t.Logf("present=%v echoes=%d attempts=%d", ok, len(d.Echoes), len(d.Attempts))
			if !ok || len(d.Echoes) != 1 || len(d.Attempts) != 1 || d.Echoes[0].Signer != operator.ReceiverPrincipal(receiverA) || d.Attempts[0].Events[0].Signer != operator.ReceiverPrincipal(receiverB) {
				t.Fatal("records predating a newly discovered end absent or unlabelled")
			}
		})
	}
}

func TestRound2RegrantGap(t *testing.T) {
	f := newFixture(t)
	at := f.now.Add(-time.Minute)
	f.operator(at.Add(time.Second), operator.Record{Kind: "end", Tenant: tenantA, Pool: poolA})
	run := f.echo(at.Add(2*time.Second), receiverA, tenantA, hostA, poolA, "gap")
	f.status(at.Add(3*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateCompleted})
	f.operator(at.Add(4*time.Second), operator.Record{Kind: "entitlement", Tenant: tenantA, Pool: poolA})
	v := f.view(time.Hour, testCaps())
	d, ok := v.RunDetail(run)
	if !ok || len(d.Echoes) != 1 || len(d.Attempts) != 1 || d.Echoes[0].Signer != operator.ReceiverPrincipal(receiverA) || d.Attempts[0].Events[0].Signer != operator.ReceiverPrincipal(receiverA) {
		t.Fatalf("records appended in an entitlement gap absent or unlabelled: present=%v echoes=%d attempts=%d", ok, len(d.Echoes), len(d.Attempts))
	}
}
