package fferr

import (
	"fmt"
	"strings"

	"github.com/rotisserie/eris"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ResourceType string

const (
	// PROVIDERS:
	EXECUTION_ERROR  = "Execution Error"
	CONNECTION_ERROR = "Connection Error"

	// DATA:
	DATASET_NOT_FOUND             = "Dataset Not Found"
	DATASET_ALREADY_EXISTS        = "Dataset Already Exists"
	DATATYPE_NOT_FOUND            = "Datatype Not Found"
	TRANSFORMATION_NOT_FOUND      = "Transformation Not Found"
	ENTITY_NOT_FOUND              = "Entity Not Found"
	FEATURE_NOT_FOUND             = "Feature Not Found"
	TRAINING_SET_NOT_FOUND        = "Training Set Not Found"
	INVALID_RESOURCE_TYPE         = "Invalid Resource Type"
	INVALID_RESOURCE_NAME_VARIANT = "Invalid Resource Name Variant"
	INVALID_FILE_TYPE             = "Invalid File Type"
	RESOURCE_CHANGED              = "Resource Changed"

	// MISCELLANEOUS:
	INTERNAL_ERROR   = "Internal Error"
	INVALID_ARGUMENT = "Invalid Argument"

	// JOBS:
	JOB_DOES_NOT_EXIST        = "Job Does Not Exist"
	JOB_ALREADY_EXISTS        = "Job Already Exists"
	RESOURCE_ALREADY_COMPLETE = "Resource Already Complete"
	RESOURCE_ALREADY_FAILED   = "Resource Already Failed"
	RESOURCE_NOT_READY        = "Resource Not Ready"
	RESOURCE_FAILED           = "Resource Failed"

	// ETCD
	KEY_NOT_FOUND = "Key Not Found"

	// RESOURCE TYPES:
	FEATURE              ResourceType = "FEATURE"
	LABEL                ResourceType = "LABEL"
	TRAINING_SET         ResourceType = "TRAINING_SET"
	SOURCE               ResourceType = "SOURCE"
	FEATURE_VARIANT      ResourceType = "FEATURE_VARIANT"
	LABEL_VARIANT        ResourceType = "LABEL_VARIANT"
	TRAINING_SET_VARIANT ResourceType = "TRAINING_SET_VARIANT"
	SOURCE_VARIANT       ResourceType = "SOURCE_VARIANT"
	PROVIDER             ResourceType = "PROVIDER"
	ENTITY               ResourceType = "ENTITY"
	MODEL                ResourceType = "MODEL"
	USER                 ResourceType = "USER"
)

type JSONStackTrace map[string]interface{}

type GRPCError interface {
	GetCode() codes.Code
	GetType() string
	ToErr() error
	AddDetail(key, value string)
	Error() string
	Stack() JSONStackTrace
}

func FromErr(err error) GRPCError {
	// If the error is nil, then simply pass it through to
	// avoid having to check for nil errors at the call site
	if err == nil {
		return nil
	}
	if grpcError, ok := err.(GRPCError); ok {
		return grpcError
	}
	st, ok := status.FromError(err)
	if !ok {
		return NewInternalError(err)
	}
	// If the error is a valid status error but doesn't have any details, it stems from a
	// location in the codebase we haven't covered yet. In this case, we'll just return an
	// InternalError
	if len(st.Details()) == 0 {
		return NewInternalError(err)
	}
	var grpcError GRPCError
	// All fferr errors should have an ErrorInfo detail, so we'll iterate through the details
	// and cast them to ErrorInfo. If we find one, we'll create the appropriate error type
	// and return it
	for _, detail := range st.Details() {
		if errorInfo, ok := detail.(*errdetails.ErrorInfo); ok {
			errorMsg := err.Error()
			// This addresses the edge case where we receive a status error from another service and persist the
			// error message to ETCD, which currently only occurs in the coordinator service. If the error message
			// contains "rpc error:", we'll just return an empty string and let the GRPCError implementation of Error()
			// handle the error message
			if strings.Contains(err.Error(), "rpc error:") {
				errorMsg = ""
			}
			grpcError = &baseGRPCError{
				code:      st.Code(),
				errorType: errorInfo.Reason,
				GenericError: GenericError{
					msg:     errorMsg,
					err:     eris.New(err.Error()),
					details: errorInfo.Metadata,
				},
			}
			// If there's a need to return a public implementation of GRPCError (e.g. InvalidArgument)
			// we can add a switch statement that checks the error type and returns the appropriate
			// error type, which will simply wrap the baseGRPCError (e.g. &InvalidArgument{baseGRPCError})
		} else {
			grpcError = NewInternalError(err)
		}
	}
	return grpcError
}

func newBaseGRPCError(err error, errorType string, code codes.Code) baseGRPCError {
	if err == nil {
		err = fmt.Errorf("initial error")
	}
	genericError := NewGenericError(err)

	return baseGRPCError{
		code:         code,
		errorType:    errorType,
		GenericError: genericError,
	}
}

type baseGRPCError struct {
	code      codes.Code
	errorType string
	GenericError
}

func (e *baseGRPCError) GetCode() codes.Code {
	return e.code
}

func (e *baseGRPCError) GetType() string {
	return string(e.errorType)
}

func (e *baseGRPCError) ToErr() error {
	st := status.New(e.code, e.msg)
	ef := &errdetails.ErrorInfo{
		Reason:   e.errorType,
		Metadata: e.details,
	}
	statusWithDetails, err := st.WithDetails(ef)
	if err == nil {
		return statusWithDetails.Err()
	}
	return st.Err()
}

func (e *baseGRPCError) AddDetail(key, value string) {
	e.GenericError.AddDetail(key, value)
}

func (e *baseGRPCError) Error() string {
	msg := fmt.Sprintf("%s: %s\n", e.errorType, e.msg)
	if len(e.details) == 0 {
		return msg
	}
	msg = fmt.Sprintf("%sDetails:\n", msg)
	for k, v := range e.details {
		msg = fmt.Sprintf("%s*%s: %s\n", msg, k, v)
	}
	return msg
}

func (e *baseGRPCError) Stack() JSONStackTrace {
	return e.GenericError.Stack()
}
