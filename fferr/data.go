package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

func NewResourceInternalError(resourceName string, resourceVariant string, resourceType ResourceType, err error) *InternalError {
	if err == nil {
		err = fmt.Errorf("internal error")
	}
	baseError := newBaseError(err, INTERNAL_ERROR, codes.Internal)
	baseError.AddDetails("resource_name", resourceName, "resource_variant", resourceVariant, "resource_type", resourceType.String())

	return &InternalError{
		baseError,
	}
}

func NewDatasetNotFoundError(resourceName, resourceVariant string, err error) *DatasetNotFoundError {
	if err == nil {
		err = fmt.Errorf("dataset not found")
	}
	baseError := newBaseError(err, DATASET_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)

	return &DatasetNotFoundError{
		baseError,
	}
}

type DatasetNotFoundError struct {
	baseError
}

func NewDatasetAlreadyExistsError(resourceName, resourceVariant string, err error) *DatasetAlreadyExistsError {
	if err == nil {
		err = fmt.Errorf("dataset already exists")
	}
	baseError := newBaseError(err, DATASET_ALREADY_EXISTS, codes.AlreadyExists)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)

	return &DatasetAlreadyExistsError{
		baseError,
	}
}

type DatasetAlreadyExistsError struct {
	baseError
}

func NewDataTypeNotFoundError(valueType string, err error) *DataTypeNotFoundError {
	if err == nil {
		err = fmt.Errorf("datatype not found")
	}
	baseError := newBaseError(err, DATATYPE_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("value_type", valueType)

	return &DataTypeNotFoundError{
		baseError,
	}
}

type DataTypeNotFoundError struct {
	baseError
}

func NewTransformationNotFoundError(resourceName, resourceVariant string, err error) *TransformationNotFoundError {
	if err == nil {
		err = fmt.Errorf("transformation not found")
	}
	baseError := newBaseError(err, TRANSFORMATION_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)

	return &TransformationNotFoundError{
		baseError,
	}
}

type TransformationNotFoundError struct {
	baseError
}

func NewEntityNotFoundError(featureName, featureVariant, entityName string, err error) *EntityNotFoundError {
	if err == nil {
		err = fmt.Errorf("entity not found")
	}
	baseError := newBaseError(err, ENTITY_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("feature_name", featureName)
	baseError.AddDetail("feature_variant", featureVariant)
	baseError.AddDetail("entity_name", entityName)

	return &EntityNotFoundError{
		baseError,
	}
}

type EntityNotFoundError struct {
	baseError
}

func NewFeatureNotFoundError(featureName, featureVariant string, err error) *FeatureNotFoundError {
	if err == nil {
		err = fmt.Errorf("feature not found")
	}
	baseError := newBaseError(err, FEATURE_NOT_FOUND, codes.NotFound)
	baseError.AddDetails("feature_name", featureName, "feature_variant", featureVariant)

	return &FeatureNotFoundError{
		baseError,
	}
}

type FeatureNotFoundError struct {
	baseError
}

func NewTrainingSetNotFoundError(resourceName, resourceVariant string, err error) *TrainingSetNotFoundError {
	if err == nil {
		err = fmt.Errorf("training set not found")
	}
	baseError := newBaseError(err, TRAINING_SET_NOT_FOUND, codes.NotFound)
	baseError.AddDetails("resource_name", resourceName, "resource_variant", resourceVariant)

	return &TrainingSetNotFoundError{
		baseError,
	}
}

type TrainingSetNotFoundError struct {
	baseError
}

func NewInvalidResourceTypeError(resourceName, resourceVariant string, resourceType ResourceType, err error) *InvalidResourceTypeError {
	if err == nil {
		err = fmt.Errorf("invalid resource type")
	}
	baseError := newBaseError(err, INVALID_RESOURCE_TYPE, codes.InvalidArgument)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)
	baseError.AddDetail("resource_type", string(resourceType))

	return &InvalidResourceTypeError{
		baseError,
	}
}

type InvalidResourceTypeError struct {
	baseError
}

func NewInvalidResourceVariantNameError(resourceName, resourceVariant string, resourceType ResourceType, err error) *InvalidResourceTypeError {
	if err == nil {
		err = fmt.Errorf("invalid resource variant name or variant")
	}
	baseError := newBaseError(err, INVALID_RESOURCE_TYPE, codes.InvalidArgument)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)
	baseError.AddDetail("resource_type", string(resourceType))

	return &InvalidResourceTypeError{
		baseError,
	}
}

type InvalidResourceNameVariantError struct {
	baseError
}

func NewInvalidFileTypeError(extension string, err error) *InvalidFileTypeError {
	if err == nil {
		err = fmt.Errorf("invalid filetype")
	}
	baseError := newBaseError(err, INVALID_FILE_TYPE, codes.InvalidArgument)
	baseError.AddDetail("extension", extension)

	return &InvalidFileTypeError{
		baseError,
	}
}

type InvalidFileTypeError struct {
	baseError
}

func NewResourceChangedError(resourceName, resourceVariant string, resourceType ResourceType, err error) *ResourceChangedError {
	if err == nil {
		err = fmt.Errorf("a resource with the same name and variant already exists but differs from the one you're trying to create; use a different variant name or autogenerated variant name")
	}
	baseError := newBaseError(err, RESOURCE_CHANGED, codes.Internal)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)
	baseError.AddDetail("resource_type", string(resourceType))

	return &ResourceChangedError{
		baseError,
	}
}

type ResourceChangedError struct {
	baseError
}
