package operator

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// operator check fails without any grant operator init gives wherry.operator.
func TestCheckRequiresTheOperatorsGrants(t *testing.T) {
	for _, acl := range OperatorACLs {
		for _, operation := range acl.Operations {
			t.Run(acl.ResourceType+"/"+operation, func(t *testing.T) {
				t.Parallel()
				_, service, closeFixture := kfakeService(t)
				defer closeFixture()
				ctx := context.Background()
				if err := service.Check(ctx); err != nil {
					t.Fatal(err)
				}
				dropped := acl
				dropped.Operations = []string{operation}
				builder, err := aclBuilder(dropped)
				if err != nil {
					t.Fatal(err)
				}
				if err := service.deleteACLs(ctx, builder); err != nil {
					t.Fatal(err)
				}
				err = service.Check(ctx)
				if want := " ACL on " + acl.Resource + " for " + OperatorPrincipal; err == nil || !strings.HasPrefix(err.Error(), "missing ") || !strings.HasSuffix(err.Error(), want) {
					t.Fatalf("check without %s on the %s = %v; want it missing", operation, acl.ResourceType, err)
				}
			})
		}
	}
}

// verifyACLs retries, within topicWait, a describe that omits an expected operation, as brokers apply an ACL creation in
// turn, and returns any other failure at once.
func TestVerifyACLsWaitsForBrokers(t *testing.T) {
	grant := ACL{TenantPrincipal("tenant-a", "host-a"), "topic", TenantTopic("tenant-a"), false, []string{"READ"}}
	create := func(t *testing.T, service *Service, acl ACL) {
		builder, err := aclBuilder(acl)
		if err != nil {
			t.Fatal(err)
		}
		results, err := kadm.NewClient(service.Operator).CreateACLs(context.Background(), builder)
		if err == nil {
			for _, result := range results {
				err = errors.Join(err, result.Err)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	// describes counts DescribeACLs requests, answering the nth itself when answer handles it.
	describes := func(cluster *kfake.Cluster, answer func(n int64, request *kmsg.DescribeACLsRequest) (kmsg.Response, error, bool)) *atomic.Int64 {
		var count atomic.Int64
		cluster.ControlKey(int16(kmsg.DescribeACLs), func(request kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			return answer(count.Add(1), request.(*kmsg.DescribeACLsRequest))
		})
		return &count
	}
	empty := func(request *kmsg.DescribeACLsRequest) kmsg.Response { return request.ResponseKind() }

	t.Run("described after an empty describe", func(t *testing.T) {
		t.Parallel()
		cluster, service, closeFixture := kfakeService(t)
		defer closeFixture()
		create(t, service, grant)
		count := describes(cluster, func(n int64, request *kmsg.DescribeACLsRequest) (kmsg.Response, error, bool) {
			return empty(request), nil, n == 1
		})
		if err := service.verifyACLs(context.Background(), []ACL{grant}); err != nil {
			t.Fatal(err)
		}
		if count.Load() < 2 {
			t.Fatalf("%d describes; want a retry", count.Load())
		}
	})

	t.Run("one operation stays absent", func(t *testing.T) {
		t.Parallel()
		_, service, closeFixture := kfakeService(t)
		defer closeFixture()
		partial := grant
		partial.Operations = []string{"READ", "WRITE"}
		create(t, service, partial)
		expected := grant
		expected.Operations = []string{"READ", "DESCRIBE", "WRITE"}
		err := service.verifyACLs(context.Background(), []ACL{expected})
		if want := "missing DESCRIBE ACL on " + grant.Resource + " for " + grant.Principal; err == nil || err.Error() != want {
			t.Fatalf("verify = %v; want %q", err, want)
		}
	})

	t.Run("never described", func(t *testing.T) {
		t.Parallel()
		_, service, closeFixture := kfakeService(t)
		defer closeFixture()
		ctx, cancel := context.WithTimeout(context.Background(), 2*topicWait)
		defer cancel()
		start := time.Now()
		err := service.verifyACLs(ctx, []ACL{grant})
		if want := "missing READ ACL on " + grant.Resource + " for " + grant.Principal; err == nil || err.Error() != want {
			t.Fatalf("verify = %v; want %q", err, want)
		}
		if elapsed := time.Since(start); elapsed < topicWait {
			t.Fatalf("verify failed after %v; want at least %v", elapsed, topicWait)
		}
	})

	// A response of another kind fails to parse, a request error kgo does not retry.
	malformed := func(int64, *kmsg.DescribeACLsRequest) kmsg.Response { return kmsg.NewPtrListGroupsResponse() }
	for _, test := range []struct {
		name       string
		operations []string
		answer     func(n int64, request *kmsg.DescribeACLsRequest) kmsg.Response
		want       error
		describes  int64
	}{
		{"request error", []string{"READ"}, malformed, nil, 1},
		{"request error after an empty describe", []string{"READ"}, func(n int64, request *kmsg.DescribeACLsRequest) kmsg.Response {
			if n == 1 {
				return empty(request)
			}
			return malformed(n, request)
		}, nil, 2},
		{"result error", []string{"READ"}, func(_ int64, request *kmsg.DescribeACLsRequest) kmsg.Response {
			response := request.ResponseKind().(*kmsg.DescribeACLsResponse)
			response.ErrorCode = kerr.SecurityDisabled.Code
			return response
		}, kerr.SecurityDisabled, 1},
		{"result error after an empty result", []string{"READ", "WRITE"}, func(_ int64, request *kmsg.DescribeACLsRequest) kmsg.Response {
			response := request.ResponseKind().(*kmsg.DescribeACLsResponse)
			if request.Operation == kmsg.ACLOperationWrite {
				response.ErrorCode = kerr.SecurityDisabled.Code
			}
			return response
		}, kerr.SecurityDisabled, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cluster, service, closeFixture := kfakeService(t)
			defer closeFixture()
			acl := grant
			acl.Operations = test.operations
			count := describes(cluster, func(n int64, request *kmsg.DescribeACLsRequest) (kmsg.Response, error, bool) {
				return test.answer(n, request), nil, true
			})
			err := service.verifyACLs(context.Background(), []ACL{acl})
			if err == nil || strings.HasPrefix(err.Error(), "missing ") || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("verify = %v; want the describe's error", err)
			}
			if count.Load() != test.describes {
				t.Fatalf("%d describes; want %d", count.Load(), test.describes)
			}
		})
	}

	t.Run("one allowance for every ACL", func(t *testing.T) {
		t.Parallel()
		cluster, service, closeFixture := kfakeService(t)
		defer closeFixture()
		create(t, service, grant)
		absent := grant
		absent.Resource = TenantTopic("tenant-b")
		start := time.Now()
		late := start.Add(topicWait * 7 / 10)
		describes(cluster, func(_ int64, request *kmsg.DescribeACLsRequest) (kmsg.Response, error, bool) {
			return empty(request), nil, request.ResourceName != nil && *request.ResourceName == grant.Resource && time.Now().Before(late)
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*topicWait)
		defer cancel()
		err := service.verifyACLs(ctx, []ACL{grant, absent})
		elapsed := time.Since(start)
		if want := "missing READ ACL on " + absent.Resource + " for " + absent.Principal; err == nil || err.Error() != want {
			t.Fatalf("verify = %v; want %q", err, want)
		}
		if elapsed < topicWait || elapsed > topicWait*3/2 {
			t.Fatalf("verify failed after %v; want about one %v allowance", elapsed, topicWait)
		}
	})

	t.Run("no ACLs", func(t *testing.T) {
		t.Parallel()
		cluster, service, closeFixture := kfakeService(t)
		defer closeFixture()
		count := describes(cluster, func(_ int64, request *kmsg.DescribeACLsRequest) (kmsg.Response, error, bool) {
			return empty(request), nil, true
		})
		if err := service.verifyACLs(context.Background(), nil); err != nil || count.Load() != 0 {
			t.Fatalf("verify = %v after %d describes; want nil after none", err, count.Load())
		}
	})

	t.Run("cancelled during the wait", func(t *testing.T) {
		t.Parallel()
		cluster, service, closeFixture := kfakeService(t)
		defer closeFixture()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cancelled := make(chan time.Time, 1)
		describes(cluster, func(n int64, request *kmsg.DescribeACLsRequest) (kmsg.Response, error, bool) {
			if n == 1 {
				time.AfterFunc(listBackoff/10, func() {
					cancelled <- time.Now()
					cancel()
				})
			}
			return empty(request), nil, true
		})
		err := service.verifyACLs(ctx, []ACL{grant})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("verify = %v; want the context's error", err)
		}
		// A wait that ignored the cancellation would return about 90ms after it, at the end of the backoff.
		if elapsed := time.Since(<-cancelled); elapsed >= listBackoff/2 {
			t.Fatalf("verify returned %v after the cancellation; want under %v", elapsed, listBackoff/2)
		}
	})
}
