// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

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

func NewDatasetLocationNotFoundError(loc string, err error) *DatasetNotFoundError {
	if err == nil {
		err = fmt.Errorf("dataset location not found")
	}
	baseError := newBaseError(err, DATASET_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("location", loc)

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

func NewDataTypeNotFoundError(value any, err error) *DataTypeNotFoundError {
	if err == nil {
		err = fmt.Errorf("datatype not found")
	}
	baseError := newBaseError(err, DATATYPE_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("value_and_type", fmt.Sprintf("%#v %T", value, value))

	return &DataTypeNotFoundError{
		baseError,
	}
}

func NewDataTypeNotFoundErrorf(value any, format string, args ...any) *DataTypeNotFoundError {
	return NewDataTypeNotFoundError(value, fmt.Errorf(format, args...))
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

type TypeError struct {
	baseError
}

func NewTypeError(valueType string, value any, err error) *TypeError {
	if err == nil {
		err = fmt.Errorf("type error")
	}
	baseError := newBaseError(err, TYPE_ERROR, codes.InvalidArgument)
	baseError.AddDetail("expected type", valueType)
	baseError.AddDetail("found type", fmt.Sprintf("%T", value))
	baseError.AddDetail("found value", fmt.Sprintf("%v", value))
	return &TypeError{
		baseError,
	}
}

func NewTypeErrorf(valueType string, value any, tmp string, args ...any) *TypeError {
	err := fmt.Errorf(tmp, args...)
	return NewTypeError(valueType, value, err)
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

func NewInvalidResourceTypeErrorf(resourceName, resourceVariant string, resourceType ResourceType, format string, a ...any) *InvalidResourceTypeError {
	err := fmt.Errorf(format, a...)
	return NewInvalidResourceTypeError(resourceName, resourceVariant, resourceType, err)
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
