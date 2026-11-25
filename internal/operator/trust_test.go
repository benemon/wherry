package operator

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// An unknown authority is any verification failure crypto/tls wraps, however the verifier words it: Go's own
// x509.UnknownAuthorityError, and the plain error darwin's platform verifier returns for a certificate it does not
// trust, both inside franz-go's dial wrap. A name mismatch, an expired certificate and an error that is no verification
// failure keep Go's text. The hint names the configuration file, and with a ca_cert set asks for no key already set.
func TestExplainUnknownAuthority(t *testing.T) {
	dial := func(err error) error {
		return fmt.Errorf("unable to dial: %w", &tls.CertificateVerificationError{Err: err})
	}
	unknown := dial(x509.UnknownAuthorityError{})
	darwin := dial(fmt.Errorf("x509: %s", `"roundtrip" certificate is not trusted`))
	for name, c := range map[string]struct {
		err      error
		detected bool
	}{
		"Go's unknown authority":           {unknown, true},
		"darwin's plain not trusted":       {darwin, true},
		"a name mismatch":                  {dial(x509.HostnameError{Host: "broker"}), false},
		"an expired certificate":           {dial(x509.CertificateInvalidError{Reason: x509.Expired}), false},
		"a refused login, no verification": {errors.New("unable to dial: SASL_AUTHENTICATION_FAILED"), false},
		"no error":                         {nil, false},
	} {
		if got := unknownAuthority(c.err); got != c.detected {
			t.Errorf("%s: unknownAuthority(%v) = %v; want %v", name, c.err, got, c.detected)
		}
		if explained := ExplainUnknownAuthority(c.err, CASetting{File: "config.yaml"}); (explained != c.err) != c.detected {
			t.Errorf("%s: explained as %v; want it explained %v", name, explained, c.detected)
		}
	}
	for _, err := range []error{unknown, darwin} {
		unset := ExplainUnknownAuthority(err, CASetting{File: "config.yaml"}).Error()
		if want := "cannot verify the broker's certificate: install the CA that signed it in this machine's trust store, or set ca_cert in config.yaml (" + err.Error() + ")"; unset != want {
			t.Errorf("without a ca_cert: %q; want %q", unset, want)
		}
		set := ExplainUnknownAuthority(err, CASetting{File: "config.yaml", Set: true}).Error()
		if want := "cannot verify the broker's certificate: it is signed neither by this machine's trust store nor by the CA in ca_cert in config.yaml (" + err.Error() + ")"; set != want {
			t.Errorf("with a ca_cert: %q; want %q", set, want)
		}
		if strings.Contains(set, "set ca_cert") {
			t.Errorf("with a ca_cert set the hint asks to set it: %q", set)
		}
		once := ExplainUnknownAuthority(err, CASetting{File: "config.yaml"})
		if !errors.Is(once, err) {
			t.Errorf("the explanation of %v no longer wraps it", err)
		}
		if twice := ExplainUnknownAuthority(fmt.Errorf("command: %w", once), CASetting{File: "other.yaml", Set: true}); !strings.HasPrefix(twice.Error(), "command: "+once.Error()) || strings.Count(twice.Error(), "cannot verify") != 1 {
			t.Errorf("a second pass over an explanation gave %q; want the first explanation kept", twice)
		}
	}
}
