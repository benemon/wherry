package signed

import (
	"crypto/ed25519"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"strings"

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
	Run     string `json:"run"`
	Turn    int64  `json:"turn"`
	Pool    string `json:"pool"`
}

type Status struct {
	Version  int    `json:"v"`
	EventID  string `json:"event_id"`
	Receiver string `json:"receiver"`
	Tenant   string `json:"tenant"`
	Run      string `json:"run"`
	Turn     int64  `json:"turn"`
	Attempt  int64  `json:"attempt"`
	Pool     string `json:"pool"`
	State    string `json:"state"`
}

func (task Task) valid() bool {
	return task.Version == 1 && task.Tenant != "" && task.Host != "" && task.Run != "" && task.Pool != "" && task.Agent != "" && task.Turn >= 0 && task.Attempt >= 0
}

func (cancel Cancel) valid() bool {
	return cancel.Version == 1 && cancel.Tenant != "" && cancel.Host != "" && cancel.Run != "" && cancel.Pool != "" && cancel.Turn >= 0
}

func (status Status) valid() bool {
	return status.Version == 1 && status.EventID != "" && status.Receiver != "" && status.Tenant != "" && status.Run != "" && status.Pool != "" && status.State != "" && status.Turn >= 0 && status.Attempt >= 0
}

func SignTask(task Task, key ed25519.PrivateKey) ([]byte, error) {
	task.Version = 1
	if !task.valid() {
		return nil, errors.New("invalid task")
	}
	return Sign(TaskType, HostOwner(task.Tenant, task.Host), task, key)
}

// ParseTask verifies a task against the current key of the host it names.
func ParseTask(encoded []byte, keys Keys) (Task, error) {
	var task Task
	owner, err := Verify(encoded, TaskType, keys, &task)
	if err != nil {
		return Task{}, err
	}
	if !task.valid() || owner != HostOwner(task.Tenant, task.Host) {
		return Task{}, errors.New("invalid task")
	}
	return task, nil
}

func SignCancel(cancel Cancel, key ed25519.PrivateKey) ([]byte, error) {
	cancel.Version = 1
	if !cancel.valid() {
		return nil, errors.New("invalid cancel")
	}
	return Sign(CancelType, HostOwner(cancel.Tenant, cancel.Host), cancel, key)
}

func ParseCancel(encoded []byte, keys Keys, runTenant string) (Cancel, error) {
	var cancel Cancel
	owner, err := Verify(encoded, CancelType, keys, &cancel)
	if err != nil {
		return Cancel{}, err
	}
	if !cancel.valid() || owner != HostOwner(cancel.Tenant, cancel.Host) || cancel.Tenant != runTenant {
		return Cancel{}, errors.New("invalid cancel")
	}
	return cancel, nil
}

func SignStatus(status Status, key ed25519.PrivateKey) ([]byte, error) {
	status.Version = 1
	if !status.valid() {
		return nil, errors.New("invalid status")
	}
	return Sign(StatusType, status.Receiver, status, key)
}

// ParseStatus verifies a status against the current key of the receiver it
// names and binds it to the run's placed pool and tenant.
func ParseStatus(encoded []byte, keys Keys, placedPool, runTenant string) (Status, error) {
	var status Status
	owner, err := Verify(encoded, StatusType, keys, &status)
	if err != nil {
		return Status{}, err
	}
	if !status.valid() || owner != status.Receiver || status.Pool != placedPool || status.Tenant != runTenant {
		return Status{}, errors.New("invalid status")
	}
	return status, nil
}

// Sign signs payload as a compact JWS of type typ; kid names the owner and
// its key so a verifier resolves the key before reading the payload.
func Sign(typ, owner string, payload any, key ed25519.PrivateKey) ([]byte, error) {
	encoded, err := json.Marshal(payload, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	options := new(jose.SignerOptions).WithType(jose.ContentType(typ)).WithHeader(jose.HeaderKey("kid"), owner+"/"+KeyID(key.Public().(ed25519.PublicKey)))
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: key}, options)
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

// Verify checks encoded against the owner's current key named by its kid and
// only then decodes the payload strictly into dst. It returns the owner, which
// the caller must match against the payload's own claims.
func Verify(encoded []byte, typ string, keys Keys, dst any) (string, error) {
	jws, err := jose.ParseSignedCompact(string(encoded), []jose.SignatureAlgorithm{jose.EdDSA})
	if err != nil {
		return "", fmt.Errorf("parse JWS: %w", err)
	}
	if len(jws.Signatures) != 1 || jws.Signatures[0].Protected.Algorithm != string(jose.EdDSA) || jws.Signatures[0].Protected.ExtraHeaders[jose.HeaderType] != typ {
		return "", errors.New("invalid JWS protected header")
	}
	owner, keyID, ok := strings.Cut(jws.Signatures[0].Protected.KeyID, "/")
	if !ok {
		return "", errors.New("JWS kid does not name an owner and key")
	}
	public, err := keys.current(owner, keyID)
	if err != nil {
		return "", err
	}
	payload, err := jws.Verify(public)
	if err != nil {
		return "", errors.New("invalid JWS signature")
	}
	if err := json.Unmarshal(payload, dst, strict); err != nil {
		return "", fmt.Errorf("decode payload: %w", err)
	}
	return owner, nil
}
