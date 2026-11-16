package record

import (
	"github.com/concrnt/concrnt"
)

func policyCreateAction(doc concrnt.Document[any]) string {
	if doc.Kind == "association" {
		return "association:create"
	}
	return "record:create"
}

func policyReadAction(doc concrnt.Document[any]) string {
	if doc.Kind == "association" {
		return "association:read"
	}
	return "record:read"
}

func policyDeleteAction(doc concrnt.Document[any]) string {
	if doc.Kind == "association" {
		return "association:delete"
	}
	return "record:delete"
}
