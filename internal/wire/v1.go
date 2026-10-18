package wire

import (
	json "encoding/json/v2"
	"errors"
)

// SelectorApproval is a version 1 enrol_approval with a non-empty receiver selector, which the current version cannot
// express. It still counts toward generation numbering but grants nothing.
type SelectorApproval struct {
	EnrolApproval `json:",inline"`
	Selector      []Requirement `json:"selector"`
}

type submissionExtensionV1 struct {
	SubmissionExtension `json:",inline"`
	Labels              map[string]string `json:"labels"`
}

type submissionRecordV1 struct {
	Message Message               `json:"message"`
	Wherry  submissionExtensionV1 `json:"wherry"`
}

type capabilityV1 struct {
	Capability `json:",inline"`
	Selector   []Requirement `json:"selector"`
}

// decodeV1 decodes a version 1 payload of a type that has since changed shape and converts it to the current type.
// Task labels and capability selectors are discarded; an approval selector is kept only as a SelectorApproval.
func decodeV1(recordType RecordType, encoded []byte) (any, error) {
	switch recordType {
	case TypeSubmission:
		record := &submissionRecordV1{Wherry: submissionExtensionV1{SubmissionExtension: SubmissionExtension{
			DeadlineMS: -1, QueueDeadlineMS: -1, Lineage: Lineage{Depth: -1, ParentHash: "\x00missing"},
		}}}
		if err := json.Unmarshal(encoded, record, strictDecodeOptions); err != nil {
			return nil, err
		}
		if record.Wherry.Labels == nil {
			return nil, errors.New("labels are required")
		}
		if err := validateTerms(record.Wherry.Labels, "labels"); err != nil {
			return nil, err
		}
		submission := &SubmissionRecord{Message: record.Message, Wherry: record.Wherry.SubmissionExtension}
		if err := validateSubmission(submission); err != nil {
			return nil, err
		}
		return submission, nil
	case TypeCapability:
		record := &capabilityV1{Capability: Capability{Capacity: -1, InFlight: -1, Generation: -1, AtMS: -1}}
		if err := json.Unmarshal(encoded, record, strictDecodeOptions); err != nil {
			return nil, err
		}
		if err := ValidateRequirements(record.Selector); err != nil {
			return nil, err
		}
		if err := validateCapability(&record.Capability); err != nil {
			return nil, err
		}
		return &record.Capability, nil
	case TypeEnrolApproval:
		record := &SelectorApproval{EnrolApproval: EnrolApproval{PrevStateHash: "\x00missing", Generation: -1, ExpMS: -1}}
		if err := json.Unmarshal(encoded, record, strictDecodeOptions); err != nil {
			return nil, err
		}
		if err := ValidateRequirements(record.Selector); err != nil {
			return nil, err
		}
		if err := validateEnrolApproval(&record.EnrolApproval); err != nil {
			return nil, err
		}
		if len(record.Selector) == 0 {
			return &record.EnrolApproval, nil
		}
		return record, nil
	default:
		return nil, errors.New("record type has no earlier version")
	}
}
