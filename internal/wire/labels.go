package wire

import (
	json "encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Requirement is one selector condition on a label.
type Requirement struct {
	Key      string   `json:"key"`
	Operator string   `json:"operator"`
	Values   []string `json:"values,omitzero"`
}

// Selector operators.
const (
	OpIn           = "In"
	OpNotIn        = "NotIn"
	OpExists       = "Exists"
	OpDoesNotExist = "DoesNotExist"
)

// Offer is an adapter a receiver can currently dispatch to and the agent it
// launches. Its labels live in the receiver's configuration record.
type Offer struct {
	Adapter string `json:"adapter"`
	Agent   string `json:"agent"`
}

// reservedPrefix is held back for labels wherry itself may define.
const reservedPrefix = "wherry.io"

var (
	labelName   = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?$`)
	labelPrefix = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$`)
)

// validKey accepts Kubernetes-style keys: an optional DNS-subdomain prefix
// and a slash, then a name of at most 63 characters.
func validKey(key string) bool {
	prefix, name, prefixed := strings.Cut(key, "/")
	if !prefixed {
		return labelName.MatchString(key)
	}
	return len(prefix) <= 253 && labelPrefix.MatchString(prefix) && prefix != reservedPrefix && !strings.HasSuffix(prefix, "."+reservedPrefix) && labelName.MatchString(name)
}

// ValidateLabels checks label keys and values.
func ValidateLabels(labels map[string]string) error {
	for key, value := range labels {
		if !validKey(key) || !labelName.MatchString(value) {
			return fmt.Errorf("label %q must have a valid key and value", key)
		}
	}
	return nil
}

// ValidateRequirements checks a selector's keys, operators and values.
func ValidateRequirements(requirements []Requirement) error {
	keys := make(map[string]bool, len(requirements))
	for _, requirement := range requirements {
		if !validKey(requirement.Key) {
			return errors.New("invalid requirement key")
		}
		if keys[requirement.Key] {
			return errors.New("duplicate requirement key")
		}
		keys[requirement.Key] = true
		switch requirement.Operator {
		case OpIn, OpNotIn:
			if len(requirement.Values) == 0 {
				return errors.New("In and NotIn requirements need values")
			}
			for i, value := range requirement.Values {
				if !labelName.MatchString(value) || slices.Contains(requirement.Values[:i], value) {
					return errors.New("requirement values must be valid and unique")
				}
			}
		case OpExists, OpDoesNotExist:
			if len(requirement.Values) != 0 {
				return errors.New("Exists and DoesNotExist requirements take no values")
			}
		default:
			return errors.New("unknown operator")
		}
	}
	return nil
}

// Satisfies reports whether labels meet every requirement of selector.
func Satisfies(selector []Requirement, labels map[string]string) bool {
	for _, requirement := range selector {
		value, ok := labels[requirement.Key]
		switch requirement.Operator {
		case OpIn:
			if !ok || !slices.Contains(requirement.Values, value) {
				return false
			}
		case OpNotIn:
			if ok && slices.Contains(requirement.Values, value) {
				return false
			}
		case OpExists:
			if !ok {
				return false
			}
		case OpDoesNotExist:
			if ok {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func validateOffers(offers []Offer) error {
	for i, offer := range offers {
		if err := validateIDs(map[string]string{"offer adapter": offer.Adapter, "offer agent": offer.Agent}); err != nil {
			return err
		}
		if slices.ContainsFunc(offers[:i], func(other Offer) bool { return other.Adapter == offer.Adapter }) {
			return errors.New("duplicate offer adapter")
		}
	}
	return nil
}

func validateArtifactUpdate(update *TaskArtifactUpdateEvent) error {
	if err := validateTaskIDs(update.TaskID, update.ContextID); err != nil {
		return err
	}
	if err := validateArtifact(&update.Artifact); err != nil {
		return err
	}
	if len(update.Artifact.Parts) != 1 {
		return errors.New("artifact_update requires exactly one part")
	}
	if part := update.Artifact.Parts[0]; part.URL != "" {
		var pointer PointerMetadata
		if part.Metadata == nil || json.Unmarshal(part.Metadata, &pointer) != nil || !validHash(pointer.SHA256) || pointer.Size <= 0 {
			return errors.New("url part metadata requires sha256 and a positive size")
		}
	}
	return validateMetadata(update.Metadata)
}

func validateTask(task *Task) error {
	if err := validateTaskIDs(task.ID, task.ContextID); err != nil {
		return err
	}
	if err := validateTaskStatus(&task.Status); err != nil {
		return err
	}
	if task.Artifacts == nil || task.History == nil {
		return errors.New("invalid task")
	}
	for i := range task.Artifacts {
		if err := validateArtifact(&task.Artifacts[i]); err != nil {
			return err
		}
	}
	for i := range task.History {
		if err := validateMessage(&task.History[i]); err != nil {
			return err
		}
	}
	return validateMetadata(task.Metadata)
}
