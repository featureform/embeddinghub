// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

// Package provider_schema contains ...
package provider_schema

import (
	"fmt"
	"strings"

	"github.com/featureform/fferr"
)

const (
	base_path       = "featureform"
	Primary         = "Primary"
	Transformation  = "Transformation"
	Feature         = "Feature"
	Materialization = "Materialization"
	Label           = "Label"
	TrainingSet     = "TrainingSet"
)

// ResourceToDirectoryPath returns the directory path for a given ResourceID in the filestore
func ResourceToDirectoryPath(resourceType, name, variant string) string {
	return fmt.Sprintf("%s/%s/%s/%s", base_path, resourceType, name, variant)
}

// ResourceToPicklePath returns the path to the pickled DataFrame transformation for a given ResourceID
func ResourceToPicklePath(name, variant string) string {
	return fmt.Sprintf("%s/DFTransformations/%s/%s/transformation.pkl", base_path, name, variant)
}

// ResourceToTableName returns the table name for a given ResourceID
func ResourceToTableName(resourceType, name, variant string) (string, error) {
	if err := ValidateResourceName(name, variant); err != nil {
		return "", err
	}
	switch resourceType {
	case Primary, Transformation, TrainingSet:
		return fmt.Sprintf("featureform_%s__%s__%s", strings.ToLower(resourceType), name, variant), nil
	case Feature, Label:
		return fmt.Sprintf("featureform_resource_%s__%s__%s", strings.ToLower(resourceType), name, variant), nil
	case Materialization:
		return fmt.Sprintf("featureform_materialization_%s__%s", name, variant), nil
	default:
		return "", fferr.NewInvalidArgumentErrorf("invalid resource type: %s", resourceType)
	}
}

func ResourceToMaterializationID(resourceType, name, variant string) (string, error) {
	if resourceType != Feature {
		return "", fferr.NewInvalidArgumentErrorf("resource type must be 'Materialization', got: %s", resourceType)
	}
	return fmt.Sprintf("%s__%s", name, variant), nil
}

func MaterializationIDToResource(materializationID string) (string, string, error) {
	parts := strings.Split(materializationID, "__")
	if len(parts) != 2 {
		return "", "", fferr.NewInvalidArgumentErrorf("invalid materialization ID: %s; expected 2 parts: name and variant", materializationID)
	}
	return parts[0], parts[1], nil
}

func ResourceToCatalogTableName(resourceType, name, variant string) (string, error) {
	if err := ValidateResourceName(name, variant); err != nil {
		return "", err
	}
	name = strings.ReplaceAll(name, "-", "_")
	variant = strings.ReplaceAll(variant, "-", "_")
	switch resourceType {
	case Primary:
		return fmt.Sprintf("primary__%s__%s", name, variant), nil
	case Transformation:
		return fmt.Sprintf("transformation__%s__%s", name, variant), nil
	case Feature:
		return fmt.Sprintf("feature__%s__%s", name, variant), nil
	default:
		return "", fferr.NewInvalidArgumentErrorf("invalid resource type: %s", resourceType)
	}
}

func TableNameToResource(tableName string) (string, string, string, error) {
	if !strings.HasPrefix(tableName, "featureform_") {
		return "", "", "", fferr.NewInvalidArgumentErrorf("invalid table name: %s; missing 'featureform_' prefix", tableName)
	}

	trimmedTableName := strings.TrimPrefix(tableName, "featureform_")

	parts := strings.Split(trimmedTableName, "__")

	if len(parts) != 3 {
		return "", "", "", fferr.NewInvalidArgumentErrorf("invalid table name: %s; expected 3 parts: resource type, name, and variant", tableName)
	}

	name := parts[1]
	variant := parts[2]

	// TODO: move resource types into provider_schema to avoid this translation
	// back and forth between the string representation of OfflineResourceType and
	// the table name version (i.e. just the lowercase the resource type).
	var resourceType string
	switch parts[0] {
	case "primary":
		resourceType = Primary
	case "transformation":
		resourceType = Transformation
	default:
		return "", "", "", fferr.NewInvalidArgumentErrorf("invalid table name: %s; invalid resource type: %s", tableName, parts[0])
	}

	return resourceType, name, variant, nil
}

func ValidateResourceName(name, variant string) error {
	errors := make([]string, 0)
	if strings.Contains(name, "__") {
		errors = append(errors, fmt.Sprintf("name cannot contain double underscores '__': %s", name))
	}
	if strings.Contains(variant, "__") {
		errors = append(errors, fmt.Sprintf("variant cannot contain double underscores '__': %s", variant))
	}

	if len(errors) > 0 {
		return fferr.NewInvalidArgumentErrorf("invalid resource name: %s", strings.Join(errors, ", "))
	}

	return nil
}
