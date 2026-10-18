package signed

import (
	"crypto/ed25519"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"

	jose "github.com/go-jose/go-jose/v4"
)

const (
	TaskType   = "application/vnd.wherry.task.v1+jws"
	StatusType = "application/vnd.wherry.status.v1+jws"
	CancelType = "application/vnd.wherry.cancel.v1+jws"
)

var strict = json.JoinOptions(json.RejectUnknownMembers(true), jsontext.AllowDuplicateNames(false))

type Task struct {
	Version int    `json:"v"`
	Tenant  string `json:"tenant"`
	Host    string `json:"host"`
	KeyID   string `json:"key_id"`
	Run     string `json:"run"`
	Turn    int64  `json:"turn"`
	Attempt int64  `json:"attempt"`
	Pool    string `json:"pool"`
	Agent   string `json:"agent"`
}

type Cancel struct {
	Version int    `json:"v"`
	Tenant  string `json:"tenant"`
	Host    string `json:"host"`
	KeyID   string `json:"key_id"`
	Run     string `json:"run"`
	Turn    int64  `json:"turn"`
	Pool    string `json:"pool"`
}

type Status struct {
	Version  int    `json:"v"`
	EventID  string `json:"event_id"`
	Receiver string `json:"receiver"`
	KeyID    string `json:"key_id"`
	Tenant   string `json:"tenant"`
	Run      string `json:"run"`
	Turn     int64  `json:"turn"`
	Attempt  int64  `json:"attempt"`
	Pool     string `json:"pool"`
	State    string `json:"state"`
}

func (task Task) valid() bool {
	return task.Version == 1 && task.Tenant != "" && task.Host != "" && task.KeyID != "" && task.Run != "" && task.Pool != "" && task.Agent != "" && task.Turn >= 0 && task.Attempt >= 0
}

func (cancel Cancel) valid() bool {
	return cancel.Version == 1 && cancel.Tenant != "" && cancel.Host != "" && cancel.KeyID != "" && cancel.Run != "" && cancel.Pool != "" && cancel.Turn >= 0
}

func (status Status) valid() bool {
	return status.Version == 1 && status.EventID != "" && status.Receiver != "" && status.KeyID != "" && status.Tenant != "" && status.Run != "" && status.Pool != "" && status.State != "" && status.Turn >= 0 && status.Attempt >= 0
}

func SignTask(task Task, key ed25519.PrivateKey) ([]byte, error) {
	task.Version, task.KeyID = 1, KeyID(key.Public().(ed25519.PublicKey))
	if !task.valid() {
		return nil, errors.New("invalid task")
	}
	return sign(TaskType, task, key)
}

// ParseTask verifies a task against the current key of the host it names.
func ParseTask(encoded []byte, keys Keys) (Task, error) {
	var task Task
	if err := verify(encoded, TaskType, &task, func() (ed25519.PublicKey, error) { return keys.current(HostOwner(task.Tenant, task.Host), task.KeyID) }); err != nil {
		return Task{}, err
	}
	if !task.valid() {
		return Task{}, errors.New("invalid task")
	}
	return task, nil
}

func SignCancel(cancel Cancel, key ed25519.PrivateKey) ([]byte, error) {
	cancel.Version, cancel.KeyID = 1, KeyID(key.Public().(ed25519.PublicKey))
	if !cancel.valid() {
		return nil, errors.New("invalid cancel")
	}
	return sign(CancelType, cancel, key)
}

func ParseCancel(encoded []byte, keys Keys, runTenant string) (Cancel, error) {
	var cancel Cancel
	if err := verify(encoded, CancelType, &cancel, func() (ed25519.PublicKey, error) {
		return keys.current(HostOwner(cancel.Tenant, cancel.Host), cancel.KeyID)
	}); err != nil {
		return Cancel{}, err
	}
	if !cancel.valid() || cancel.Tenant != runTenant {
		return Cancel{}, errors.New("invalid cancel")
	}
	return cancel, nil
}

func SignStatus(status Status, key ed25519.PrivateKey) ([]byte, error) {
	status.Version, status.KeyID = 1, KeyID(key.Public().(ed25519.PublicKey))
	if !status.valid() {
		return nil, errors.New("invalid status")
	}
	return sign(StatusType, status, key)
}

// ParseStatus verifies a status against the current key of the receiver it
// names and binds it to the run's placed pool and tenant.
func ParseStatus(encoded []byte, keys Keys, placedPool, runTenant string) (Status, error) {
	var status Status
	if err := verify(encoded, StatusType, &status, func() (ed25519.PublicKey, error) { return keys.current(status.Receiver, status.KeyID) }); err != nil {
		return Status{}, err
	}
	if !status.valid() || status.Pool != placedPool || status.Tenant != runTenant {
		return Status{}, errors.New("invalid status")
	}
	return status, nil
}

func sign(typ string, payload any, key ed25519.PrivateKey) ([]byte, error) {
	encoded, err := json.Marshal(payload, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: key}, new(jose.SignerOptions).WithType(jose.ContentType(typ)))
	if err != nil {
		return nil, err
	}
	jws, err := signer.Sign(encoded)
	if err != nil {
		return nil, err
	}
	compact, err := jws.CompactSerialize()
	return []byte(compact), err
}

// verify decodes the unverified payload only to learn which key to resolve;
// dst is trusted after the signature checks against that key.
func verify(encoded []byte, typ string, dst any, key func() (ed25519.PublicKey, error)) error {
	jws, err := jose.ParseSignedCompact(string(encoded), []jose.SignatureAlgorithm{jose.EdDSA})
	if err != nil {
		return fmt.Errorf("parse JWS: %w", err)
	}
	if len(jws.Signatures) != 1 || jws.Signatures[0].Protected.Algorithm != string(jose.EdDSA) || jws.Signatures[0].Protected.ExtraHeaders[jose.HeaderType] != typ {
		return errors.New("invalid JWS protected header")
	}
	if err := json.Unmarshal(jws.UnsafePayloadWithoutVerification(), dst, strict); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	public, err := key()
	if err != nil {
		return err
	}
	if _, err := jws.Verify(public); err != nil {
		return errors.New("invalid JWS signature")
	}
	return nil
}
