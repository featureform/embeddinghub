// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/featureform/helpers"
	"github.com/featureform/logging"

	"github.com/featureform/fferr"
	"github.com/featureform/lib"

	"github.com/pkg/errors"

	"golang.org/x/exp/slices"

	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/metadata/search"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

const TIME_FORMAT = time.RFC1123

type operation int

const (
	create_op operation = iota
)

type ResourceType int32

const (
	FEATURE              ResourceType = ResourceType(pb.ResourceType_FEATURE)
	FEATURE_VARIANT                   = ResourceType(pb.ResourceType_FEATURE_VARIANT)
	LABEL                             = ResourceType(pb.ResourceType_LABEL)
	LABEL_VARIANT                     = ResourceType(pb.ResourceType_LABEL_VARIANT)
	USER                              = ResourceType(pb.ResourceType_USER)
	ENTITY                            = ResourceType(pb.ResourceType_ENTITY)
	PROVIDER                          = ResourceType(pb.ResourceType_PROVIDER)
	SOURCE                            = ResourceType(pb.ResourceType_SOURCE)
	SOURCE_VARIANT                    = ResourceType(pb.ResourceType_SOURCE_VARIANT)
	TRAINING_SET                      = ResourceType(pb.ResourceType_TRAINING_SET)
	TRAINING_SET_VARIANT              = ResourceType(pb.ResourceType_TRAINING_SET_VARIANT)
	MODEL                             = ResourceType(pb.ResourceType_MODEL)
)

func (r ResourceType) ToLoggingResourceType() logging.ResourceType {
	switch r {
	case FEATURE:
		return logging.Feature
	case FEATURE_VARIANT:
		return logging.FeatureVariant
	case LABEL:
		return logging.Label
	case LABEL_VARIANT:
		return logging.LabelVariant
	case USER:
		return logging.User
	case ENTITY:
		return logging.Entity
	case PROVIDER:
		return logging.Provider
	case SOURCE:
		return logging.Source
	case SOURCE_VARIANT:
		return logging.SourceVariant
	case TRAINING_SET:
		return logging.TrainingSet
	case TRAINING_SET_VARIANT:
		return logging.TrainingSetVariant
	case MODEL:
		return logging.Model
	default:
		return ""

	}
}

func (r ResourceType) String() string {
	return pb.ResourceType_name[int32(r)]
}

func (r ResourceType) Serialized() pb.ResourceType {
	return pb.ResourceType(r)
}

type ResourceStatus int32

const (
	NO_STATUS ResourceStatus = ResourceStatus(pb.ResourceStatus_NO_STATUS)
	CREATED                  = ResourceStatus(pb.ResourceStatus_CREATED)
	PENDING                  = ResourceStatus(pb.ResourceStatus_PENDING)
	READY                    = ResourceStatus(pb.ResourceStatus_READY)
	FAILED                   = ResourceStatus(pb.ResourceStatus_FAILED)
)

func (r ResourceStatus) String() string {
	return pb.ResourceStatus_Status_name[int32(r)]
}

func (r ResourceStatus) Serialized() pb.ResourceStatus_Status {
	return pb.ResourceStatus_Status(r)
}

type ComputationMode int32

const (
	PRECOMPUTED     ComputationMode = ComputationMode(pb.ComputationMode_PRECOMPUTED)
	CLIENT_COMPUTED                 = ComputationMode(pb.ComputationMode_CLIENT_COMPUTED)
)

func (cm ComputationMode) Equals(mode pb.ComputationMode) bool {
	return cm == ComputationMode(mode)
}

func (cm ComputationMode) String() string {
	return pb.ComputationMode_name[int32(cm)]
}

var parentMapping = map[ResourceType]ResourceType{
	FEATURE_VARIANT:      FEATURE,
	LABEL_VARIANT:        LABEL,
	SOURCE_VARIANT:       SOURCE,
	TRAINING_SET_VARIANT: TRAINING_SET,
}

func (serv *MetadataServer) needsJob(res Resource) bool {
	if res.ID().Type == TRAINING_SET_VARIANT ||
		res.ID().Type == SOURCE_VARIANT ||
		res.ID().Type == LABEL_VARIANT {
		return true
	}
	if res.ID().Type == FEATURE_VARIANT {
		if fv, ok := res.(*featureVariantResource); !ok {
			serv.Logger.Errorf("resource has type FEATURE VARIANT but failed to cast %s", res.ID())
			return false
		} else {
			return PRECOMPUTED.Equals(fv.serialized.Mode)
		}
	}
	return false
}

type ResourceID struct {
	Name    string
	Variant string
	Type    ResourceType
}

func (id ResourceID) Proto() *pb.NameVariant {
	return &pb.NameVariant{
		Name:    id.Name,
		Variant: id.Variant,
	}
}

func (id ResourceID) Parent() (ResourceID, bool) {
	parentType, has := parentMapping[id.Type]
	if !has {
		return ResourceID{}, false
	}
	return ResourceID{
		Name: id.Name,
		Type: parentType,
	}, true
}

func (id ResourceID) String() string {
	if id.Variant == "" {
		return fmt.Sprintf("%s %s", id.Type, id.Name)
	}
	return fmt.Sprintf("%s %s (%s)", id.Type, id.Name, id.Variant)
}

var bannedStrings = [...]string{"__"}
var bannedPrefixes = [...]string{"_"}
var bannedSuffixes = [...]string{"_"}

func resourceNamedSafely(id ResourceID) error {
	for _, substr := range bannedStrings {
		if strings.Contains(id.Name, substr) {
			return fferr.NewInvalidResourceVariantNameError(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), fmt.Errorf("resource name contains banned string %s", substr))
		}
		if strings.Contains(id.Variant, substr) {
			return fferr.NewInvalidResourceVariantNameError(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), fmt.Errorf("resource variant %s contains banned string %s", id.Name, substr))
		}
	}
	for _, substr := range bannedPrefixes {
		if strings.HasPrefix(id.Name, substr) {
			return fferr.NewInvalidResourceVariantNameError(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), fmt.Errorf("resource name %s contains banned prefix %s", id.Name, substr))
		}
		if strings.HasPrefix(id.Variant, substr) {
			return fferr.NewInvalidResourceVariantNameError(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), fmt.Errorf("resource variant %s contains banned prefix %s", id.Name, substr))
		}
	}
	for _, substr := range bannedSuffixes {
		if strings.HasSuffix(id.Name, substr) {
			return fferr.NewInvalidResourceVariantNameError(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), fmt.Errorf("resource name %s contains banned suffix %s", id.Name, substr))
		}
		if strings.HasSuffix(id.Variant, substr) {
			return fferr.NewInvalidResourceVariantNameError(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), fmt.Errorf("resource variant %s contains banned suffix %s", id.Name, substr))
		}
	}
	return nil
}

type ResourceVariant interface {
	IsEquivalent(ResourceVariant) (bool, error)
	ToResourceVariantProto() *pb.ResourceVariant
}

type Resource interface {
	Notify(ResourceLookup, operation, Resource) error
	ID() ResourceID
	Schedule() string
	Dependencies(ResourceLookup) (ResourceLookup, error)
	Proto() proto.Message
	GetStatus() *pb.ResourceStatus
	UpdateStatus(pb.ResourceStatus) error
	UpdateSchedule(string) error
	Update(ResourceLookup, Resource) error
}

func isDirectDependency(lookup ResourceLookup, dependency, parent Resource) (bool, error) {
	depId := dependency.ID()
	deps, depsErr := parent.Dependencies(lookup)
	if depsErr != nil {
		return false, depsErr
	}
	return deps.Has(depId)
}

type ResourceLookup interface {
	Lookup(context.Context, ResourceID) (Resource, error)
	Has(ResourceID) (bool, error)
	Set(ResourceID, Resource) error
	Submap([]ResourceID) (ResourceLookup, error)
	ListForType(ResourceType) ([]Resource, error)
	List() ([]Resource, error)
	HasJob(ResourceID) (bool, error)
	SetJob(ResourceID, string) error
	SetStatus(context.Context, ResourceID, pb.ResourceStatus) error
	SetSchedule(ResourceID, string) error
}

type SearchWrapper struct {
	Searcher search.Searcher
	ResourceLookup
}

func (wrapper SearchWrapper) Set(id ResourceID, res Resource) error {
	if err := wrapper.ResourceLookup.Set(id, res); err != nil {
		return err
	}

	var allTags []string
	switch res.(type) {
	case *sourceVariantResource:
		allTags = res.(*sourceVariantResource).serialized.Tags.Tag

	case *featureVariantResource:
		allTags = res.(*featureVariantResource).serialized.Tags.Tag

	case *labelVariantResource:
		allTags = res.(*labelVariantResource).serialized.Tags.Tag

	case *trainingSetVariantResource:
		allTags = res.(*trainingSetVariantResource).serialized.Tags.Tag
	}

	doc := search.ResourceDoc{
		Name:    id.Name,
		Type:    id.Type.String(),
		Tags:    allTags,
		Variant: id.Variant,
	}
	return wrapper.Searcher.Upsert(doc)
}

type LocalResourceLookup map[ResourceID]Resource

func (lookup LocalResourceLookup) Lookup(ctx context.Context, id ResourceID) (Resource, error) {
	logger := logging.GetLoggerFromContext(ctx)
	res, has := lookup[id]
	if !has {
		wrapped := fferr.NewKeyNotFoundError(id.String(), nil)
		wrapped.AddDetail("resource_type", id.Type.String())
		logger.Errorw("resource not found", "resource ID", id.String(), "error", wrapped)
		return nil, wrapped
	}
	return res, nil
}

func (lookup LocalResourceLookup) Has(id ResourceID) (bool, error) {
	_, has := lookup[id]
	return has, nil
}

func (lookup LocalResourceLookup) Set(id ResourceID, res Resource) error {
	lookup[id] = res
	return nil
}

func (lookup LocalResourceLookup) Submap(ids []ResourceID) (ResourceLookup, error) {
	resources := make(LocalResourceLookup, len(ids))
	for _, id := range ids {
		resource, has := lookup[id]
		if !has {
			wrapped := fferr.NewDatasetNotFoundError(id.Name, id.Variant, fmt.Errorf("resource not found"))
			wrapped.AddDetail("resource_type", id.Type.String())
			return nil, wrapped
		}
		resources[id] = resource
	}
	return resources, nil
}

func (lookup LocalResourceLookup) ListForType(t ResourceType) ([]Resource, error) {
	resources := make([]Resource, 0)
	for id, res := range lookup {
		if id.Type == t {
			resources = append(resources, res)
		}
	}
	return resources, nil
}

func (lookup LocalResourceLookup) List() ([]Resource, error) {
	resources := make([]Resource, 0, len(lookup))
	for _, res := range lookup {
		resources = append(resources, res)
	}
	return resources, nil
}

func (lookup LocalResourceLookup) SetStatus(ctx context.Context, id ResourceID, status pb.ResourceStatus) error {
	res, has := lookup[id]
	if !has {
		wrapped := fferr.NewDatasetNotFoundError(id.Name, id.Variant, fmt.Errorf("resource not found"))
		wrapped.AddDetail("resource_type", id.Type.String())
		return wrapped
	}
	if err := res.UpdateStatus(status); err != nil {
		return err
	}
	lookup[id] = res
	return nil
}

func (lookup LocalResourceLookup) SetJob(id ResourceID, schedule string) error {
	return nil
}

func (lookup LocalResourceLookup) SetSchedule(id ResourceID, schedule string) error {
	res, has := lookup[id]
	if !has {
		wrapped := fferr.NewDatasetNotFoundError(id.Name, id.Variant, fmt.Errorf("resource not found"))
		wrapped.AddDetail("resource_type", id.Type.String())
		return wrapped
	}
	if err := res.UpdateSchedule(schedule); err != nil {
		return err
	}
	lookup[id] = res
	return nil
}

func (lookup LocalResourceLookup) HasJob(id ResourceID) (bool, error) {
	return false, nil
}

type SourceResource struct {
	serialized *pb.Source
}

func (resource *SourceResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: SOURCE,
	}
}

func (resource *SourceResource) Schedule() string {
	return ""
}

func (resource *SourceResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *SourceResource) Proto() proto.Message {
	return resource.serialized
}

func (this *SourceResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	otherId := that.ID()
	isVariant := otherId.Type == SOURCE_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return nil
	}
	if slices.Contains(this.serialized.Variants, otherId.Variant) {
		fmt.Printf("source %s already has variant %s\n", this.serialized.Name, otherId.Variant)
		return nil
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
	return nil
}

func (resource *SourceResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *SourceResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *SourceResource) UpdateSchedule(schedule string) error {
	return fferr.NewInternalError(fmt.Errorf("not implemented"))
}

func (resource *SourceResource) Update(lookup ResourceLookup, updateRes Resource) error {
	wrapped := fferr.NewDatasetAlreadyExistsError(resource.ID().Name, resource.ID().Variant, nil)
	wrapped.AddDetail("resource_type", resource.ID().Type.String())
	return wrapped
}

type sourceVariantResource struct {
	serialized *pb.SourceVariant
}

func (resource *sourceVariantResource) ID() ResourceID {
	return ResourceID{
		Name:    resource.serialized.Name,
		Variant: resource.serialized.Variant,
		Type:    SOURCE_VARIANT,
	}
}

func (resource *sourceVariantResource) Schedule() string {
	return resource.serialized.Schedule
}

func (resource *sourceVariantResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name: serialized.Owner,
			Type: USER,
		},
		{
			Name: serialized.Provider,
			Type: PROVIDER,
		},
		{
			Name: serialized.Name,
			Type: SOURCE,
		},
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (resource *sourceVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (sourceVariantResource *sourceVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	id := that.ID()
	t := id.Type
	key := id.Proto()
	serialized := sourceVariantResource.serialized
	switch t {
	case TRAINING_SET_VARIANT:
		serialized.Trainingsets = append(serialized.Trainingsets, key)
	case FEATURE_VARIANT:
		serialized.Features = append(serialized.Features, key)
	case LABEL_VARIANT:
		serialized.Labels = append(serialized.Labels, key)
	}
	return nil
}

func (resource *sourceVariantResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *sourceVariantResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.LastUpdated = tspb.Now()
	resource.serialized.Status = &status
	return nil
}

func (resource *sourceVariantResource) UpdateSchedule(schedule string) error {
	resource.serialized.Schedule = schedule
	return nil
}

func (resource *sourceVariantResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	variantUpdate, ok := deserialized.(*pb.SourceVariant)
	if !ok {
		return fferr.NewInternalError(fmt.Errorf("failed to deserialize existing source variant record"))
	}
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, variantUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, variantUpdate.Properties)
	return nil
}

func (resource *sourceVariantResource) IsEquivalent(other ResourceVariant) (bool, error) {
	/**
	Key Fields for a SourceVariant are
	- Name
	- Definition
	- Owner
	- Provider
	*/
	otherVariant, ok := other.(*sourceVariantResource)
	if !ok {
		return false, nil
	}

	thisProto := resource.serialized
	otherProto := otherVariant.serialized

	isDefinitionEqual := false
	var err error
	switch thisDef := thisProto.Definition.(type) {
	case *pb.SourceVariant_Transformation:
		if otherDef, ok := otherProto.Definition.(*pb.SourceVariant_Transformation); ok {
			isDefinitionEqual, err = isSourceProtoDefinitionEqual(thisDef, otherDef)
			if err != nil {
				return false, fferr.NewInternalError(fmt.Errorf("error comparing source definitions: %v", err))
			}

		}
	case *pb.SourceVariant_PrimaryData:
		if otherDef, ok := otherProto.Definition.(*pb.SourceVariant_PrimaryData); ok {
			isDefinitionEqual = proto.Equal(thisDef.PrimaryData, otherDef.PrimaryData)
		}
	}

	if thisProto.GetName() == otherProto.GetName() &&
		thisProto.GetOwner() == otherProto.GetOwner() &&
		thisProto.GetProvider() == otherProto.GetProvider() &&
		isDefinitionEqual {

		return true, nil
	}
	return false, nil
}

func isSourceProtoDefinitionEqual(thisDef, otherDef *pb.SourceVariant_Transformation) (bool, error) {
	isDefinitionEqual := false
	switch thisDef.Transformation.Type.(type) {
	case *pb.Transformation_DFTransformation:
		if otherDef, ok := otherDef.Transformation.Type.(*pb.Transformation_DFTransformation); ok {
			sourceTextEqual := thisDef.Transformation.GetDFTransformation().SourceText == otherDef.DFTransformation.SourceText
			inputsEqual, err := lib.EqualProtoContents(thisDef.Transformation.GetDFTransformation().Inputs, otherDef.DFTransformation.Inputs)
			if err != nil {
				return false, fferr.NewInternalError(fmt.Errorf("error comparing transformation inputs: %v", err))
			}
			isDefinitionEqual = sourceTextEqual &&
				inputsEqual
		}
	case *pb.Transformation_SQLTransformation:
		if _, ok := otherDef.Transformation.Type.(*pb.Transformation_SQLTransformation); ok {
			isDefinitionEqual = thisDef.Transformation.GetSQLTransformation().Query == otherDef.Transformation.GetSQLTransformation().Query
		}
	}

	kubernetesArgsEqual := proto.Equal(thisDef.Transformation.GetKubernetesArgs(), otherDef.Transformation.GetKubernetesArgs())
	return isDefinitionEqual && kubernetesArgsEqual, nil
}

func (resource *sourceVariantResource) ToResourceVariantProto() *pb.ResourceVariant {
	return &pb.ResourceVariant{Resource: &pb.ResourceVariant_SourceVariant{SourceVariant: resource.serialized}}
}

type featureResource struct {
	serialized *pb.Feature
}

func (resource *featureResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: FEATURE,
	}
}

func (resource *featureResource) Schedule() string {
	return ""
}

func (resource *featureResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *featureResource) Proto() proto.Message {
	return resource.serialized
}

func (this *featureResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	otherId := that.ID()
	isVariant := otherId.Type == FEATURE_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return nil
	}
	if slices.Contains(this.serialized.Variants, otherId.Variant) {
		fmt.Printf("source %s already has variant %s\n", this.serialized.Name, otherId.Variant)
		return nil
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
	return nil
}

func (resource *featureResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *featureResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *featureResource) UpdateSchedule(schedule string) error {
	return fferr.NewInternalError(fmt.Errorf("not implemented"))
}

func (resource *featureResource) Update(lookup ResourceLookup, updateRes Resource) error {
	wrapped := fferr.NewDatasetAlreadyExistsError(resource.ID().Name, resource.ID().Variant, nil)
	wrapped.AddDetail("resource_type", resource.ID().Type.String())
	return wrapped
}

type featureVariantResource struct {
	serialized *pb.FeatureVariant
}

func (resource *featureVariantResource) ID() ResourceID {
	return ResourceID{
		Name:    resource.serialized.Name,
		Variant: resource.serialized.Variant,
		Type:    FEATURE_VARIANT,
	}
}

func (resource *featureVariantResource) Schedule() string {
	return resource.serialized.Schedule
}

func (resource *featureVariantResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name: serialized.Owner,
			Type: USER,
		},
		{
			Name: serialized.Name,
			Type: FEATURE,
		},
	}
	if PRECOMPUTED.Equals(serialized.Mode) {
		depIds = append(depIds, ResourceID{
			Name:    serialized.Source.Name,
			Variant: serialized.Source.Variant,
			Type:    SOURCE_VARIANT,
		},
			ResourceID{
				Name: serialized.Entity,
				Type: ENTITY,
			})

		// Only add the Provider if it is non-empty
		if serialized.Provider != "" {
			depIds = append(depIds, ResourceID{
				Name: serialized.Provider,
				Type: PROVIDER,
			})
		}
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (resource *featureVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *featureVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	if !PRECOMPUTED.Equals(this.serialized.Mode) {
		return nil
	}
	id := that.ID()
	relevantOp := op == create_op && id.Type == TRAINING_SET_VARIANT
	if !relevantOp {
		return nil
	}
	key := id.Proto()
	this.serialized.Trainingsets = append(this.serialized.Trainingsets, key)
	return nil
}

func (resource *featureVariantResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *featureVariantResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.LastUpdated = tspb.Now()
	resource.serialized.Status = &status
	return nil
}

func (resource *featureVariantResource) UpdateSchedule(schedule string) error {
	resource.serialized.Schedule = schedule
	return nil
}

func (resource *featureVariantResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	variantUpdate, ok := deserialized.(*pb.FeatureVariant)
	if !ok {
		return fferr.NewInternalError(fmt.Errorf("failed to deserialize existing feature variant record"))
	}
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, variantUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, variantUpdate.Properties)
	return nil
}

func (resource *featureVariantResource) IsEquivalent(other ResourceVariant) (bool, error) {
	/**
	Key Fields for a FeatureVariant are
	- Name
	- Source
	- Function
	- Provider
	- Entity
	- Owner
	- Type
	*/

	otherVariant, ok := other.(*featureVariantResource)
	if !ok {
		return false, nil
	}

	thisProto := resource.serialized
	otherProto := otherVariant.serialized

	isEquivalentLocation := false
	if thisProto.GetFunction() != nil {
		isEquivalentLocation = proto.Equal(thisProto.GetFunction(), otherProto.GetFunction())
	} else {
		isEquivalentLocation = proto.Equal(thisProto.GetColumns(), otherProto.GetColumns())
	}

	if thisProto.GetName() == otherProto.GetName() &&
		proto.Equal(thisProto.GetSource(), otherProto.GetSource()) &&
		thisProto.GetProvider() == otherProto.GetProvider() &&
		thisProto.GetEntity() == otherProto.GetEntity() &&
		proto.Equal(thisProto.GetType(), otherProto.GetType()) &&
		isEquivalentLocation &&
		thisProto.Owner == otherProto.Owner {

		return true, nil
	}
	return false, nil
}

func (resource *featureVariantResource) ToResourceVariantProto() *pb.ResourceVariant {
	return &pb.ResourceVariant{Resource: &pb.ResourceVariant_FeatureVariant{FeatureVariant: resource.serialized}}
}

func (resource *featureVariantResource) GetDefinition() string {
	params := resource.serialized.GetAdditionalParameters().GetFeatureType()
	if params == nil {
		return ""
	}
	ondemand, isOnDemand := params.(*pb.FeatureParameters_Ondemand)
	if !isOnDemand {
		return ""
	}
	return ondemand.Ondemand.GetDefinition()
}

type labelResource struct {
	serialized *pb.Label
}

func (resource *labelResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: LABEL,
	}
}

func (resource *labelResource) Schedule() string {
	return ""
}

func (resource *labelResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *labelResource) Proto() proto.Message {
	return resource.serialized
}

func (this *labelResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	otherId := that.ID()
	isVariant := otherId.Type == LABEL_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return nil
	}
	if slices.Contains(this.serialized.Variants, otherId.Variant) {
		fmt.Printf("source %s already has variant %s\n", this.serialized.Name, otherId.Variant)
		return nil
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
	return nil
}

func (resource *labelResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *labelResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *labelResource) UpdateSchedule(schedule string) error {
	return fferr.NewInternalError(fmt.Errorf("not implemented"))
}

func (resource *labelResource) Update(lookup ResourceLookup, updateRes Resource) error {
	wrapped := fferr.NewDatasetAlreadyExistsError(resource.ID().Name, resource.ID().Variant, nil)
	wrapped.AddDetail("resource_type", resource.ID().Type.String())
	return wrapped
}

type labelVariantResource struct {
	serialized *pb.LabelVariant
}

func (resource *labelVariantResource) ID() ResourceID {
	return ResourceID{
		Name:    resource.serialized.Name,
		Variant: resource.serialized.Variant,
		Type:    LABEL_VARIANT,
	}
}

func (resource *labelVariantResource) Schedule() string {
	return ""
}

func (resource *labelVariantResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name:    serialized.Source.Name,
			Variant: serialized.Source.Variant,
			Type:    SOURCE_VARIANT,
		},
		{
			Name: serialized.Entity,
			Type: ENTITY,
		},
		{
			Name: serialized.Owner,
			Type: USER,
		},
		{
			Name: serialized.Provider,
			Type: PROVIDER,
		},
		{
			Name: serialized.Name,
			Type: LABEL,
		},
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (resource *labelVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *labelVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	id := that.ID()
	releventOp := op == create_op && id.Type == TRAINING_SET_VARIANT
	if !releventOp {
		return nil
	}
	key := id.Proto()
	this.serialized.Trainingsets = append(this.serialized.Trainingsets, key)
	return nil
}

func (resource *labelVariantResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *labelVariantResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *labelVariantResource) UpdateSchedule(schedule string) error {
	return fferr.NewInternalError(fmt.Errorf("not implemented"))
}

func (resource *labelVariantResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	variantUpdate, ok := deserialized.(*pb.LabelVariant)
	if !ok {
		return fferr.NewInternalError(fmt.Errorf("failed to deserialize existing label variant record"))
	}
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, variantUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, variantUpdate.Properties)
	return nil
}

func (resource *labelVariantResource) IsEquivalent(other ResourceVariant) (bool, error) {
	/**
	Key Fields for a LabelVariant are
	- Name
	- Source
	- Columns
	- Owner
	- Entity
	- Type
	*/
	otherVariant, ok := other.(*labelVariantResource)
	if !ok {
		return false, nil
	}

	thisProto := resource.serialized
	otherProto := otherVariant.serialized

	if thisProto.GetName() == otherProto.GetName() &&
		proto.Equal(thisProto.GetSource(), otherProto.GetSource()) &&
		proto.Equal(thisProto.GetColumns(), otherProto.GetColumns()) &&
		thisProto.Entity == otherProto.Entity &&
		proto.Equal(thisProto.GetType(), otherProto.GetType()) &&
		thisProto.Owner == otherProto.Owner {

		return true, nil
	}

	return false, nil
}

func (resource *labelVariantResource) ToResourceVariantProto() *pb.ResourceVariant {
	return &pb.ResourceVariant{Resource: &pb.ResourceVariant_LabelVariant{LabelVariant: resource.serialized}}
}

type trainingSetResource struct {
	serialized *pb.TrainingSet
}

func (resource *trainingSetResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: TRAINING_SET,
	}
}

func (resource *trainingSetResource) Schedule() string {
	return ""
}

func (resource *trainingSetResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *trainingSetResource) Proto() proto.Message {
	return resource.serialized
}

func (this *trainingSetResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	otherId := that.ID()
	isVariant := otherId.Type == TRAINING_SET_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return nil
	}
	if slices.Contains(this.serialized.Variants, otherId.Variant) {
		fmt.Printf("source %s already has variant %s\n", this.serialized.Name, otherId.Variant)
		return nil
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
	return nil
}

func (resource *trainingSetResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *trainingSetResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *trainingSetResource) UpdateSchedule(schedule string) error {
	return fferr.NewInternalError(fmt.Errorf("not implemented"))
}

func (resource *trainingSetResource) Update(lookup ResourceLookup, updateRes Resource) error {
	wrapped := fferr.NewDatasetAlreadyExistsError(resource.ID().Name, resource.ID().Variant, nil)
	wrapped.AddDetail("resource_type", resource.ID().Type.String())
	return wrapped
}

type trainingSetVariantResource struct {
	serialized *pb.TrainingSetVariant
}

func (resource *trainingSetVariantResource) ID() ResourceID {
	return ResourceID{
		Name:    resource.serialized.Name,
		Variant: resource.serialized.Variant,
		Type:    TRAINING_SET_VARIANT,
	}
}

func (resource *trainingSetVariantResource) Schedule() string {
	return resource.serialized.Schedule
}

func (resource *trainingSetVariantResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name: serialized.Owner,
			Type: USER,
		},
		{
			Name: serialized.Provider,
			Type: PROVIDER,
		},
		{
			Name:    serialized.Label.Name,
			Variant: serialized.Label.Variant,
			Type:    LABEL_VARIANT,
		},
		{
			Name: serialized.Name,
			Type: TRAINING_SET,
		},
	}
	for _, feature := range serialized.Features {
		depIds = append(depIds, ResourceID{
			Name:    feature.Name,
			Variant: feature.Variant,
			Type:    FEATURE_VARIANT,
		})
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (resource *trainingSetVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *trainingSetVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	return nil
}

func (resource *trainingSetVariantResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *trainingSetVariantResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.LastUpdated = tspb.Now()
	resource.serialized.Status = &status
	return nil
}

func (resource *trainingSetVariantResource) UpdateSchedule(schedule string) error {
	resource.serialized.Schedule = schedule
	return nil
}

func (resource *trainingSetVariantResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	variantUpdate, ok := deserialized.(*pb.TrainingSetVariant)
	if !ok {
		return fferr.NewInternalError(fmt.Errorf("failed to deserialize existing training set variant record"))
	}
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, variantUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, variantUpdate.Properties)
	return nil
}

func (resource *trainingSetVariantResource) IsEquivalent(other ResourceVariant) (bool, error) {
	/**
	Key Fields for a TrainingSetVariant are
	- Name
	- Labels
	- Features
	- Lag Features
	- Owner
	*/
	otherVariant, ok := other.(*trainingSetVariantResource)
	if !ok {
		return false, nil
	}

	thisProto := resource.serialized
	otherProto := otherVariant.serialized

	equivalentLabals := proto.Equal(thisProto.GetLabel(), otherProto.GetLabel())
	equivalentFeatures := lib.EqualProtoSlices(thisProto.GetFeatures(), otherProto.GetFeatures())
	equivalentLagFeatures := lib.EqualProtoSlices(thisProto.GetFeatureLags(), otherProto.GetFeatureLags())

	if thisProto.GetName() == otherProto.GetName() &&
		equivalentLabals &&
		equivalentFeatures &&
		equivalentLagFeatures &&
		thisProto.Owner == otherProto.Owner {

		return true, nil
	}
	return false, nil
}

func (resource *trainingSetVariantResource) ToResourceVariantProto() *pb.ResourceVariant {
	return &pb.ResourceVariant{Resource: &pb.ResourceVariant_TrainingSetVariant{TrainingSetVariant: resource.serialized}}
}

type modelResource struct {
	serialized *pb.Model
}

func (resource *modelResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: MODEL,
	}
}

func (resource *modelResource) Schedule() string {
	return ""
}

func (resource *modelResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	serialized := resource.serialized
	depIds := make([]ResourceID, 0)
	for _, feature := range serialized.Features {
		depIds = append(depIds, ResourceID{
			Name:    feature.Name,
			Variant: feature.Variant,
			Type:    FEATURE_VARIANT,
		})
	}
	for _, label := range serialized.Labels {
		depIds = append(depIds, ResourceID{
			Name:    label.Name,
			Variant: label.Variant,
			Type:    LABEL_VARIANT,
		})
	}
	for _, ts := range serialized.Trainingsets {
		depIds = append(depIds, ResourceID{
			Name:    ts.Name,
			Variant: ts.Variant,
			Type:    TRAINING_SET_VARIANT,
		})
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (resource *modelResource) Proto() proto.Message {
	return resource.serialized
}

func (this *modelResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	return nil
}

func (resource *modelResource) GetStatus() *pb.ResourceStatus {
	return &pb.ResourceStatus{Status: pb.ResourceStatus_NO_STATUS}
}

func (resource *modelResource) UpdateStatus(status pb.ResourceStatus) error {
	return nil
}

func (resource *modelResource) UpdateSchedule(schedule string) error {
	return fferr.NewInternalError(fmt.Errorf("not implemented"))
}

func (resource *modelResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	modelUpdate, ok := deserialized.(*pb.Model)
	if !ok {
		return fferr.NewInternalError(fmt.Errorf("failed to deserialize existing model record"))
	}
	resource.serialized.Features = unionNameVariants(resource.serialized.Features, modelUpdate.Features)
	resource.serialized.Trainingsets = unionNameVariants(resource.serialized.Trainingsets, modelUpdate.Trainingsets)
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, modelUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, modelUpdate.Properties)
	return nil
}

type userResource struct {
	serialized *pb.User
}

func (resource *userResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: USER,
	}
}

func (resource *userResource) Schedule() string {
	return ""
}

func (resource *userResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *userResource) Proto() proto.Message {
	return resource.serialized
}

func (this *userResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	if isDep, err := isDirectDependency(lookup, this, that); err != nil {
		return err
	} else if !isDep {
		return nil
	}
	id := that.ID()
	key := id.Proto()
	t := id.Type
	serialized := this.serialized
	switch t {
	case TRAINING_SET_VARIANT:
		serialized.Trainingsets = append(serialized.Trainingsets, key)
	case FEATURE_VARIANT:
		serialized.Features = append(serialized.Features, key)
	case LABEL_VARIANT:
		serialized.Labels = append(serialized.Labels, key)
	case SOURCE_VARIANT:
		serialized.Sources = append(serialized.Sources, key)
	}
	return nil
}

func (resource *userResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *userResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *userResource) UpdateSchedule(schedule string) error {
	return fferr.NewInternalError(fmt.Errorf("not implemented"))
}

func (resource *userResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	userUpdate, ok := deserialized.(*pb.User)
	if !ok {
		return fferr.NewInternalError(errors.New("failed to deserialize existing user record"))
	}
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, userUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, userUpdate.Properties)
	return nil
}

type providerResource struct {
	serialized *pb.Provider
}

func (resource *providerResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: PROVIDER,
	}
}

func (resource *providerResource) Schedule() string {
	return ""
}

func (resource *providerResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *providerResource) Proto() proto.Message {
	return resource.serialized
}

func (this *providerResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	if isDep, err := isDirectDependency(lookup, this, that); err != nil {
		return err
	} else if !isDep {
		return nil
	}
	id := that.ID()
	key := id.Proto()
	t := id.Type
	serialized := this.serialized
	switch t {
	case SOURCE_VARIANT:
		serialized.Sources = append(serialized.Sources, key)
	case FEATURE_VARIANT:
		serialized.Features = append(serialized.Features, key)
	case TRAINING_SET_VARIANT:
		serialized.Trainingsets = append(serialized.Trainingsets, key)
	case LABEL_VARIANT:
		serialized.Labels = append(serialized.Labels, key)
	}
	return nil
}

func (resource *providerResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *providerResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *providerResource) UpdateSchedule(schedule string) error {
	return fferr.NewInternalError(fmt.Errorf("not implemented"))
}

func (resource *providerResource) Update(lookup ResourceLookup, resourceUpdate Resource) error {
	logger := logging.NewLogger("metadata-update")
	logger.Debugw("Update provider resource", "Provider resource", resource, "Resource update", resourceUpdate)
	providerUpdate, ok := resourceUpdate.Proto().(*pb.Provider)
	if !ok {
		err := fferr.NewInternalError(errors.New("failed to deserialize existing provider record"))
		logger.Errorw("Failed to deserialize existing provider record", "providerUpdate", providerUpdate, "error", err)
		return err
	}
	isValid, err := resource.isValidConfigUpdate(providerUpdate.SerializedConfig)
	if err != nil {
		logger.Errorw("Failed to validate config update", "is valid", isValid, "error", err)
		return err
	}
	if !isValid {
		wrapped := fferr.NewResourceInternalError(resource.ID().Name, resource.ID().Variant, fferr.ResourceType(resource.ID().Type.String()), fmt.Errorf("invalid config update"))
		logger.Errorw("Invalid config update", "providerUpdate", providerUpdate, "error", wrapped)
		return wrapped
	}
	resource.serialized.SerializedConfig = providerUpdate.SerializedConfig
	resource.serialized.Description = providerUpdate.Description
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, providerUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, providerUpdate.Properties)
	return nil
}

func (resource *providerResource) isValidConfigUpdate(configUpdate pc.SerializedConfig) (bool, error) {
	switch pt.Type(resource.serialized.Type) {
	case pt.BigQueryOffline:
		return isValidBigQueryConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.CassandraOnline:
		return isValidCassandraConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.DynamoDBOnline:
		return isValidDynamoConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.FirestoreOnline:
		return isValidFirestoreConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.MongoDBOnline:
		return isValidMongoConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.PostgresOffline:
		return isValidPostgresConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.ClickHouseOffline:
		return isValidClickHouseConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.RedisOnline:
		return isValidRedisConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.SnowflakeOffline:
		return isValidSnowflakeConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.RedshiftOffline:
		return isValidRedshiftConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.K8sOffline:
		return isValidK8sConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.SparkOffline:
		return isValidSparkConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.S3, pt.HDFS, pt.GCS, pt.AZURE, pt.BlobOnline:
		return true, nil
	default:
		return false, fferr.NewInternalError(fmt.Errorf("unable to update config for provider. Provider type %s not found", resource.serialized.Type))
	}
}

type entityResource struct {
	serialized *pb.Entity
}

func (resource *entityResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: ENTITY,
	}
}

func (resource *entityResource) Schedule() string {
	return ""
}

func (resource *entityResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *entityResource) Proto() proto.Message {
	return resource.serialized
}

func (this *entityResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	id := that.ID()
	key := id.Proto()
	t := id.Type
	serialized := this.serialized
	switch t {
	case TRAINING_SET_VARIANT:
		serialized.Trainingsets = append(serialized.Trainingsets, key)
	case FEATURE_VARIANT:
		serialized.Features = append(serialized.Features, key)
	case LABEL_VARIANT:
		serialized.Labels = append(serialized.Labels, key)
	}
	return nil
}

func (resource *entityResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *entityResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *entityResource) UpdateSchedule(schedule string) error {
	return fferr.NewInternalError(fmt.Errorf("not implemented"))
}

func (resource *entityResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	entityUpdate, ok := deserialized.(*pb.Entity)
	if !ok {
		return fferr.NewInternalError(errors.New("failed to deserialize existing training entity record"))
	}
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, entityUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, entityUpdate.Properties)
	return nil
}

type MetadataServer struct {
	Logger     logging.Logger
	lookup     ResourceLookup
	address    string
	grpcServer *grpc.Server
	listener   net.Listener
	pb.UnimplementedMetadataServer
}

func NewMetadataServer(config *Config) (*MetadataServer, error) {
	config.Logger.Infow("Creating new metadata server", "Address:", config.Address)
	lookup, err := config.StorageProvider.GetResourceLookup()

	if err != nil {
		config.Logger.Errorw("Could not get resource lookup", "error", err.Error())
		return nil, err
	}
	if config.SearchParams != nil {
		searcher, errInitializeSearch := search.NewMeilisearch(config.SearchParams)
		if errInitializeSearch != nil {
			config.Logger.Errorw("Could not initialize Meili search", "error", errInitializeSearch.Error())
			return nil, errInitializeSearch
		}
		lookup = &SearchWrapper{
			Searcher:       searcher,
			ResourceLookup: lookup,
		}
	}
	return &MetadataServer{
		lookup:  lookup,
		address: config.Address,
		Logger:  config.Logger,
	}, nil
}

func (serv *MetadataServer) Serve() error {
	if serv.grpcServer != nil {
		return fferr.NewInternalError(fmt.Errorf("server already running"))
	}
	lis, err := net.Listen("tcp", serv.address)
	if err != nil {
		return fferr.NewInternalError(fmt.Errorf("cannot listen to server address %s", serv.address))
	}
	return serv.ServeOnListener(lis)
}

func (serv *MetadataServer) ServeOnListener(lis net.Listener) error {
	serv.listener = lis
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(helpers.UnaryServerErrorInterceptor), grpc.StreamInterceptor(helpers.StreamServerErrorInterceptor))
	pb.RegisterMetadataServer(grpcServer, serv)
	serv.grpcServer = grpcServer
	serv.Logger.Infow("Server starting", "Address", serv.listener.Addr().String())
	return grpcServer.Serve(lis)
}

func (serv *MetadataServer) GracefulStop() error {
	if serv.grpcServer == nil {
		return fferr.NewInternalError(fmt.Errorf("server not running"))
	}
	serv.grpcServer.GracefulStop()
	serv.grpcServer = nil
	serv.listener = nil
	return nil
}

func (serv *MetadataServer) Stop() error {
	if serv.grpcServer == nil {
		return fferr.NewInternalError(fmt.Errorf("server not running"))
	}
	serv.grpcServer.Stop()
	serv.grpcServer = nil
	serv.listener = nil
	return nil
}

type StorageProvider interface {
	GetResourceLookup() (ResourceLookup, error)
}

type LocalStorageProvider struct {
}

func (sp LocalStorageProvider) GetResourceLookup() (ResourceLookup, error) {
	lookup := make(LocalResourceLookup)
	return lookup, nil
}

type EtcdStorageProvider struct {
	Config EtcdConfig
}

func (sp EtcdStorageProvider) GetResourceLookup() (ResourceLookup, error) {
	client, err := sp.Config.InitClient()
	if err != nil {
		return nil, err
	}
	lookup := EtcdResourceLookup{
		Connection: EtcdStorage{
			Client: client,
		},
	}

	return lookup, nil
}

type Config struct {
	Logger          logging.Logger
	SearchParams    *search.MeilisearchParams
	StorageProvider StorageProvider
	Address         string
}

func (serv *MetadataServer) RequestScheduleChange(ctx context.Context, req *pb.ScheduleChangeRequest) (*pb.Empty, error) {
	resID := ResourceID{Name: req.ResourceId.Resource.Name, Variant: req.ResourceId.Resource.Variant, Type: ResourceType(req.ResourceId.ResourceType)}
	err := serv.lookup.SetSchedule(resID, req.Schedule)
	return &pb.Empty{}, err
}

func (serv *MetadataServer) SetResourceStatus(ctx context.Context, req *pb.SetStatusRequest) (*pb.Empty, error) {
	logger := logging.GetLoggerFromContext(ctx)
	logger.Infow("Setting resource status", "resource_id", req.ResourceId, "status", req.Status.Status)
	resID := ResourceID{Name: req.ResourceId.Resource.Name, Variant: req.ResourceId.Resource.Variant, Type: ResourceType(req.ResourceId.ResourceType)}
	err := serv.lookup.SetStatus(ctx, resID, *req.Status)
	if err != nil {
		logger.Errorw("Could not set resource status", "error", err.Error())
	}
	return &pb.Empty{}, err
}

func (serv *MetadataServer) ListFeatures(request *pb.ListRequest, stream pb.Metadata_ListFeaturesServer) error {
	ctx := logging.AttachRequestID(request.RequestId, stream.Context(), serv.Logger)
	logging.GetLoggerFromContext(ctx).Info("Opened List Features stream")
	return serv.genericList(ctx, FEATURE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Feature))
	})
}

func (serv *MetadataServer) CreateFeatureVariant(ctx context.Context, variantRequest *pb.FeatureVariantRequest) (*pb.Empty, error) {
	ctx = logging.AttachRequestID(variantRequest.RequestId, ctx, serv.Logger)
	logger := logging.GetLoggerFromContext(ctx).WithResource(logging.FeatureVariant, variantRequest.FeatureVariant.Name, variantRequest.FeatureVariant.Variant)
	logger.Info("Creating Feature Variant")

	variant := variantRequest.FeatureVariant
	variant.Created = tspb.New(time.Now())
	return serv.genericCreate(ctx, &featureVariantResource{variant}, func(name, variant string) Resource {
		return &featureResource{
			&pb.Feature{
				Name:           name,
				DefaultVariant: variant,
				// This will be set when the change is propagated to dependencies.
				Variants: []string{},
			},
		}
	})
}

func (serv *MetadataServer) GetFeatures(stream pb.Metadata_GetFeaturesServer) error {
	ctx := logging.AddLoggerToContext(stream.Context(), serv.Logger)
	serv.Logger.Info("Opened Get Features stream")
	return serv.genericGet(ctx, stream, FEATURE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Feature))
	})
}

func (serv *MetadataServer) GetFeatureVariants(stream pb.Metadata_GetFeatureVariantsServer) error {
	ctx := logging.AddLoggerToContext(stream.Context(), serv.Logger)
	serv.Logger.Info("Opened Get Feature Variants stream")
	return serv.genericGet(ctx, stream, FEATURE_VARIANT, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.FeatureVariant))
	})
}

func (serv *MetadataServer) ListLabels(request *pb.ListRequest, stream pb.Metadata_ListLabelsServer) error {
	ctx := logging.AttachRequestID(request.RequestId, stream.Context(), serv.Logger)
	logging.GetLoggerFromContext(ctx).Info("Opened List Labels stream")
	return serv.genericList(ctx, LABEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Label))
	})
}

func (serv *MetadataServer) CreateLabelVariant(ctx context.Context, variantRequest *pb.LabelVariantRequest) (*pb.Empty, error) {
	ctx = logging.AttachRequestID(variantRequest.RequestId, ctx, serv.Logger)
	logger := logging.GetLoggerFromContext(ctx).WithResource(logging.LabelVariant, variantRequest.LabelVariant.Name, variantRequest.LabelVariant.Variant)
	logger.Info("Creating Label Variant")

	variant := variantRequest.LabelVariant
	variant.Created = tspb.New(time.Now())
	return serv.genericCreate(ctx, &labelVariantResource{variant}, func(name, variant string) Resource {
		return &labelResource{
			&pb.Label{
				Name:           name,
				DefaultVariant: variant,
				// This will be set when the change is propagated to dependencies.
				Variants: []string{},
			},
		}
	})
}

func (serv *MetadataServer) GetLabels(stream pb.Metadata_GetLabelsServer) error {
	ctx := logging.AddLoggerToContext(stream.Context(), serv.Logger)
	serv.Logger.Info("Opened Get Labels stream")
	return serv.genericGet(ctx, stream, LABEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Label))
	})
}

func (serv *MetadataServer) GetLabelVariants(stream pb.Metadata_GetLabelVariantsServer) error {
	ctx := logging.AddLoggerToContext(stream.Context(), serv.Logger)
	serv.Logger.Info("Opened Get Label Variants stream")
	return serv.genericGet(ctx, stream, LABEL_VARIANT, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.LabelVariant))
	})
}

func (serv *MetadataServer) ListTrainingSets(request *pb.ListRequest, stream pb.Metadata_ListTrainingSetsServer) error {
	ctx := logging.AttachRequestID(request.RequestId, stream.Context(), serv.Logger)
	logging.GetLoggerFromContext(ctx).Info("Opened List Training Sets stream")
	return serv.genericList(ctx, TRAINING_SET, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.TrainingSet))
	})
}

func (serv *MetadataServer) CreateTrainingSetVariant(ctx context.Context, variantRequest *pb.TrainingSetVariantRequest) (*pb.Empty, error) {
	ctx = logging.AttachRequestID(variantRequest.RequestId, ctx, serv.Logger)
	logger := logging.GetLoggerFromContext(ctx).WithResource(logging.TrainingSetVariant, variantRequest.TrainingSetVariant.Name, variantRequest.TrainingSetVariant.Variant)
	logger.Info("Creating TrainingSet Variant")

	variant := variantRequest.TrainingSetVariant
	variant.Created = tspb.New(time.Now())
	return serv.genericCreate(ctx, &trainingSetVariantResource{variant}, func(name, variant string) Resource {
		return &trainingSetResource{
			&pb.TrainingSet{
				Name:           name,
				DefaultVariant: variant,
				// This will be set when the change is propagated to dependencies.
				Variants: []string{},
			},
		}
	})
}

func (serv *MetadataServer) GetTrainingSets(stream pb.Metadata_GetTrainingSetsServer) error {
	ctx := logging.AddLoggerToContext(stream.Context(), serv.Logger)
	serv.Logger.Info("Opened Get Training Sets stream")
	return serv.genericGet(ctx, stream, TRAINING_SET, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.TrainingSet))
	})
}

func (serv *MetadataServer) GetTrainingSetVariants(stream pb.Metadata_GetTrainingSetVariantsServer) error {
	ctx := logging.AddLoggerToContext(stream.Context(), serv.Logger)
	serv.Logger.Info("Opened Get Training Set Variants stream")
	return serv.genericGet(ctx, stream, TRAINING_SET_VARIANT, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.TrainingSetVariant))
	})
}

func (serv *MetadataServer) ListSources(request *pb.ListRequest, stream pb.Metadata_ListSourcesServer) error {
	ctx := logging.AttachRequestID(request.RequestId, stream.Context(), serv.Logger)
	logging.GetLoggerFromContext(ctx).Info("Opened List Sources stream")
	return serv.genericList(ctx, SOURCE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Source))
	})
}

func (serv *MetadataServer) CreateSourceVariant(ctx context.Context, variantRequest *pb.SourceVariantRequest) (*pb.Empty, error) {
	ctx = logging.AttachRequestID(variantRequest.RequestId, ctx, serv.Logger)
	logger := logging.GetLoggerFromContext(ctx).WithResource(logging.SourceVariant, variantRequest.SourceVariant.Name, variantRequest.SourceVariant.Variant)
	logger.Info("Creating Source Variant")

	variant := variantRequest.SourceVariant
	variant.Created = tspb.New(time.Now())
	return serv.genericCreate(ctx, &sourceVariantResource{variant}, func(name, variant string) Resource {
		return &SourceResource{
			&pb.Source{
				Name:           name,
				DefaultVariant: variant,
				// This will be set when the change is propagated to dependencies.
				Variants: []string{},
			},
		}
	})
}

func (serv *MetadataServer) GetSources(stream pb.Metadata_GetSourcesServer) error {
	ctx := logging.AddLoggerToContext(stream.Context(), serv.Logger)
	serv.Logger.Info("Opened Get Sources stream")
	return serv.genericGet(ctx, stream, SOURCE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Source))
	})
}

func (serv *MetadataServer) GetSourceVariants(stream pb.Metadata_GetSourceVariantsServer) error {
	ctx := logging.AddLoggerToContext(stream.Context(), serv.Logger)
	serv.Logger.Info("Opened Get Source Variants stream")
	return serv.genericGet(ctx, stream, SOURCE_VARIANT, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.SourceVariant))
	})
}

func (serv *MetadataServer) ListUsers(request *pb.ListRequest, stream pb.Metadata_ListUsersServer) error {
	ctx := logging.AttachRequestID(request.RequestId, stream.Context(), serv.Logger)
	logging.GetLoggerFromContext(ctx).Info("Opened List Users stream")
	return serv.genericList(ctx, USER, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.User))
	})
}

func (serv *MetadataServer) CreateUser(ctx context.Context, userRequest *pb.UserRequest) (*pb.Empty, error) {
	ctx = logging.AttachRequestID(userRequest.RequestId, ctx, serv.Logger)
	logger := logging.GetLoggerFromContext(ctx).WithResource(logging.User, userRequest.User.Name, logging.NoVariant)
	logger.Info("Creating User")

	return serv.genericCreate(ctx, &userResource{userRequest.User}, nil)
}

func (serv *MetadataServer) GetUsers(stream pb.Metadata_GetUsersServer) error {
	ctx := logging.AddLoggerToContext(stream.Context(), serv.Logger)
	serv.Logger.Info("Opened Get Users stream")
	return serv.genericGet(ctx, stream, USER, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.User))
	})
}

func (serv *MetadataServer) ListProviders(request *pb.ListRequest, stream pb.Metadata_ListProvidersServer) error {
	ctx := logging.AttachRequestID(request.RequestId, stream.Context(), serv.Logger)
	logging.GetLoggerFromContext(ctx).Info("Opened List Providers stream")
	return serv.genericList(ctx, PROVIDER, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Provider))
	})
}

func (serv *MetadataServer) CreateProvider(ctx context.Context, providerRequest *pb.ProviderRequest) (*pb.Empty, error) {
	ctx = logging.AttachRequestID(providerRequest.RequestId, ctx, serv.Logger)
	logger := logging.GetLoggerFromContext(ctx).
		WithResource("provider", providerRequest.Provider.Name, "").
		WithProvider(providerRequest.Provider.Type, providerRequest.Provider.Name)
	logger.Info("Creating Provider")
	return serv.genericCreate(ctx, &providerResource{providerRequest.Provider}, nil)
}

func (serv *MetadataServer) GetProviders(stream pb.Metadata_GetProvidersServer) error {
	ctx := logging.AddLoggerToContext(stream.Context(), serv.Logger)
	serv.Logger.Info("Opened Get Providers stream")
	return serv.genericGet(ctx, stream, PROVIDER, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Provider))
	})
}

func (serv *MetadataServer) ListEntities(request *pb.ListRequest, stream pb.Metadata_ListEntitiesServer) error {
	ctx := logging.AttachRequestID(request.RequestId, stream.Context(), serv.Logger)
	logging.GetLoggerFromContext(ctx).Info("Opened List Entities stream")
	return serv.genericList(ctx, ENTITY, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Entity))
	})
}

func (serv *MetadataServer) CreateEntity(ctx context.Context, entityRequest *pb.EntityRequest) (*pb.Empty, error) {
	ctx = logging.AttachRequestID(entityRequest.RequestId, ctx, serv.Logger)
	logger := logging.GetLoggerFromContext(ctx).WithResource(logging.Entity, entityRequest.Entity.Name, logging.NoVariant)
	logger.Info("Creating Entity")
	return serv.genericCreate(ctx, &entityResource{entityRequest.Entity}, nil)
}

func (serv *MetadataServer) GetEntities(stream pb.Metadata_GetEntitiesServer) error {
	ctx := logging.AddLoggerToContext(stream.Context(), serv.Logger)
	serv.Logger.Info("Opened Get Entities stream")
	return serv.genericGet(ctx, stream, ENTITY, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Entity))
	})
}

func (serv *MetadataServer) ListModels(request *pb.ListRequest, stream pb.Metadata_ListModelsServer) error {
	ctx := logging.AttachRequestID(request.RequestId, stream.Context(), serv.Logger)
	logging.GetLoggerFromContext(ctx).Info("Opened List Models stream")
	return serv.genericList(ctx, MODEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Model))
	})
}

func (serv *MetadataServer) CreateModel(ctx context.Context, modelRequest *pb.ModelRequest) (*pb.Empty, error) {
	ctx = logging.AttachRequestID(modelRequest.RequestId, ctx, serv.Logger)
	logger := logging.GetLoggerFromContext(ctx).WithResource(logging.Model, modelRequest.Model.Name, logging.NoVariant)
	logger.Info("Creating Model")
	return serv.genericCreate(ctx, &modelResource{modelRequest.Model}, nil)
}

func (serv *MetadataServer) GetModels(stream pb.Metadata_GetModelsServer) error {
	ctx := logging.AddLoggerToContext(stream.Context(), serv.Logger)
	serv.Logger.Info("Opened Get Models stream")
	return serv.genericGet(ctx, stream, MODEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Model))
	})
}

// GetEquivalent attempts to find an equivalent resource based on the provided ResourceVariant.
func (serv *MetadataServer) GetEquivalent(ctx context.Context, req *pb.ResourceVariantRequest) (*pb.ResourceVariant, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Info("Getting Equivalent Resource Variant, %v", req.ResourceVariant.Resource)
	return serv.getEquivalent(ctx, req.ResourceVariant, true)
}

/*
*
This method is used to get the equivalent resource variant for a given resource variant. readyStatus is used to determine
if we should only return the equivalent resource variant if it is ready.
*/
func (serv *MetadataServer) getEquivalent(ctx context.Context, req *pb.ResourceVariant, filterReadyStatus bool) (*pb.ResourceVariant, error) {
	noEquivalentResponse := &pb.ResourceVariant{}
	logger := logging.GetLoggerFromContext(ctx)
	currentResource, resourceType, err := serv.extractResourceVariant(req)
	if err != nil {
		logger.Errorw("Error extracting resource variant", "resource variant", req, "error", err)
		return nil, err
	}
	resourcesForType, err := serv.lookup.ListForType(resourceType)
	if err != nil {
		logger.Errorw("Unable to list resources", "error", err)
		return nil, err
	}

	equivalentResourceVariant, err := findEquivalent(resourcesForType, currentResource, filterReadyStatus)
	if err != nil {
		logger.Errorw("Unable to find equivalent resource", "error", err)
		return nil, err
	}

	if equivalentResourceVariant == nil {
		return noEquivalentResponse, nil
	}

	return equivalentResourceVariant.ToResourceVariantProto(), nil
}

// findEquivalent searches through a slice of Resources to find an equivalent ResourceVariant.
func findEquivalent(resources []Resource, resource ResourceVariant, filterReadyStatus bool) (ResourceVariant, error) {
	for _, res := range resources {
		// If we are filtering by ready status, we only want to return the equivalent resource variant if it is ready.
		if filterReadyStatus && !isResourceReady(res) {
			continue
		}

		other, ok := res.(ResourceVariant)
		if !ok {
			return nil, fferr.NewInvalidResourceTypeError(res.ID().Name, res.ID().Variant, fferr.ResourceType(res.ID().Type.String()), fmt.Errorf("resource is not a ResourceVariant: %T", res))
		}

		equivalent, err := resource.IsEquivalent(other)
		if err != nil {
			return nil, fferr.NewInternalError(err)
		}
		if equivalent {
			return other, nil
		}
	}
	return nil, nil
}

// extractResourceVariant takes a ResourceVariant request and extracts the concrete type and corresponding ResourceType.
func (serv *MetadataServer) extractResourceVariant(req *pb.ResourceVariant) (ResourceVariant, ResourceType, error) {
	switch res := req.Resource.(type) {
	case *pb.ResourceVariant_SourceVariant:
		return &sourceVariantResource{res.SourceVariant}, SOURCE_VARIANT, nil
	case *pb.ResourceVariant_FeatureVariant:
		return &featureVariantResource{res.FeatureVariant}, FEATURE_VARIANT, nil
	case *pb.ResourceVariant_LabelVariant:
		return &labelVariantResource{res.LabelVariant}, LABEL_VARIANT, nil
	case *pb.ResourceVariant_TrainingSetVariant:
		return &trainingSetVariantResource{res.TrainingSetVariant}, TRAINING_SET_VARIANT, nil
	default:
		return nil, 0, fferr.NewInvalidArgumentError(fmt.Errorf("unknown resource variant type: %T", req.Resource))
	}
}

// isResourceReady checks if a Resource's status is 'ready'.
func isResourceReady(res Resource) bool {
	resourceStatus := res.GetStatus()
	return resourceStatus != nil && resourceStatus.Status == pb.ResourceStatus_READY
}

type nameStream interface {
	Recv() (*pb.NameRequest, error)
}

type variantStream interface {
	Recv() (*pb.NameVariantRequest, error)
}

type sendFn func(proto.Message) error

type initParentFn func(name, variant string) Resource

func (serv *MetadataServer) genericCreate(ctx context.Context, res Resource, init initParentFn) (*pb.Empty, error) {
	logger := logging.GetLoggerFromContext(ctx).WithResource(res.ID().Type.ToLoggingResourceType(), res.ID().Name, res.ID().Variant)
	logger.Debugw("Creating Generic Resource: ", res.ID().Name, res.ID().Variant)

	id := res.ID()
	if err := resourceNamedSafely(id); err != nil {
		logger.Errorw("Resource name is not valid", "error", err)
		return nil, err
	}
	existing, err := serv.lookup.Lookup(ctx, id)
	if _, isResourceError := err.(*fferr.KeyNotFoundError); err != nil && !isResourceError {
		logger.Errorw("Error looking up resource", "resource ID", id, "error", err)
		// TODO: consider checking the GRPCError interface to avoid double wrapping error
		return nil, fferr.NewInternalError(err)
	}

	if existing != nil {
		err = serv.validateExisting(res, existing)
		if err != nil {
			logger.Errorw("ID exists but is not equivalent", "error", err)
			return nil, err
		}
		if err := existing.Update(serv.lookup, res); err != nil {
			logger.Errorw("Error updating existing resource", "error", err)
			return nil, err
		}
		res = existing
	}
	if serv.needsJob(res) && existing == nil {
		logger.Info("Creating Job")
		if err := serv.lookup.SetJob(id, res.Schedule()); err != nil {
			return nil, err
		}
		logger.Info("Successfully Created Job")
	}
	// Create the parent first. Better to have a hanging parent than a hanging dependency.
	parentId, hasParent := id.Parent()
	if hasParent {
		parentExists, err := serv.lookup.Has(parentId)
		if err != nil {
			logger.Errorw("Parent does not exist", "parent-id", parentId, "error", err)
			return nil, err
		}

		if !parentExists {
			logger.Debug("Parent does not exist, creating new parent")
			parent := init(id.Name, id.Variant)
			err = serv.lookup.Set(parentId, parent)
			if err != nil {
				logger.Errorw("Unable to create new parent", "parent-id", parentId, "error", err)
				return nil, err
			}
		} else {
			if err := serv.setDefaultVariant(ctx, parentId, res.ID().Variant); err != nil {
				logger.Errorw("Error setting default variant", "parent-id", parentId, "variant", res.ID().Variant, "error", err)
				return nil, err
			}
		}
	}
	if err := serv.lookup.Set(id, res); err != nil {
		logger.Errorw("Error setting resource to lookup", "error", err)
		return nil, err
	}
	if existing == nil {
		if err := serv.propagateChange(res); err != nil {
			logger.Error(err)
			return nil, err
		}
	}
	return &pb.Empty{}, nil
}

func (serv *MetadataServer) setDefaultVariant(ctx context.Context, id ResourceID, defaultVariant string) error {
	parent, err := serv.lookup.Lookup(ctx, id)
	if err != nil {
		return err
	}
	var parentResource Resource
	if resource, ok := parent.(*SourceResource); ok {
		resource.serialized.DefaultVariant = defaultVariant
		parentResource = resource
	}
	if resource, ok := parent.(*labelResource); ok {
		resource.serialized.DefaultVariant = defaultVariant
		parentResource = resource
	}
	if resource, ok := parent.(*featureResource); ok {
		resource.serialized.DefaultVariant = defaultVariant
		parentResource = resource
	}
	if resource, ok := parent.(*trainingSetResource); ok {
		resource.serialized.DefaultVariant = defaultVariant
		parentResource = resource
	}
	err = serv.lookup.Set(id, parentResource)
	if err != nil {
		return err
	}
	return nil
}
func (serv *MetadataServer) validateExisting(newRes Resource, existing Resource) error {
	// It's possible we found a resource with the same name and variant but different contents, if different contents
	// we'll let the user know to ideally use a different variant
	// i.e. user tries to register transformation with same name and variant but different definition.
	_, isResourceVariant := newRes.(ResourceVariant)
	if isResourceVariant {
		isEquivalent, err := serv.isEquivalent(newRes, existing)
		if err != nil {
			return err
		}
		if !isEquivalent {
			return fferr.NewResourceChangedError(newRes.ID().Name, newRes.ID().Variant, fferr.ResourceType(newRes.ID().Type), nil)
		}
	}
	return nil
}

func (serv *MetadataServer) isEquivalent(newRes Resource, existing Resource) (bool, error) {
	// only works on resource variants right now
	if existing.GetStatus() != nil && existing.GetStatus().Status != pb.ResourceStatus_READY {
		return false, nil
	}

	resVariant, ok := newRes.(ResourceVariant)
	if !ok {
		return false, nil
	}
	existingVariant, ok := existing.(ResourceVariant)
	if !ok {
		return false, nil
	}
	isEquivalent, err := resVariant.IsEquivalent(existingVariant)
	if err != nil {
		return false, fferr.NewInternalError(err)
	}
	return isEquivalent, nil
}

func (serv *MetadataServer) propagateChange(newRes Resource) error {
	visited := make(map[ResourceID]struct{})
	// We have to make it a var so that the anonymous function can call itself.
	var propagateChange func(parent Resource) error
	propagateChange = func(parent Resource) error {
		deps, err := parent.Dependencies(serv.lookup)
		if err != nil {
			return err
		}
		depList, err := deps.List()
		if err != nil {
			return err
		}
		for _, res := range depList {
			id := res.ID()
			if _, has := visited[id]; has {
				continue
			}
			visited[id] = struct{}{}
			if err := res.Notify(serv.lookup, create_op, newRes); err != nil {
				return err
			}
			if err := serv.lookup.Set(res.ID(), res); err != nil {
				return err
			}
			if err := propagateChange(res); err != nil {
				return err
			}
		}
		return nil
	}
	return propagateChange(newRes)
}

func (serv *MetadataServer) genericGet(ctx context.Context, stream interface{}, t ResourceType, send sendFn) error {
	logger := logging.GetLoggerFromContext(ctx)
	for {
		var recvErr error
		var id ResourceID
		var loggerWithResource logging.Logger
		switch casted := stream.(type) {
		case nameStream:
			req, err := casted.Recv()
			recvErr = err
			if recvErr == io.EOF {
				logger.Debugw("End of stream reached. Stream request completed")
				return nil
			}
			if err != nil {
				logger.Errorw("Unable to receive request", "error", recvErr)
				return fferr.NewInternalError(recvErr)
			}
			id = ResourceID{
				Name: req.GetName().Name,
				Type: t,
			}
			ctx = logging.AttachRequestID(req.GetRequestId(), ctx, logger)
			loggerWithResource = logging.GetLoggerFromContext(ctx).WithResource(id.Type.ToLoggingResourceType(), id.Name, logging.NoVariant)
		case variantStream:
			req, err := casted.Recv()
			recvErr = err
			if recvErr == io.EOF {
				logger.Debugw("End of stream reached. Stream request completed")
				return nil
			}
			if err != nil {
				logger.Errorw("Unable to receive request", "error", recvErr)
				return fferr.NewInternalError(recvErr)
			}
			id = ResourceID{
				Name:    req.GetNameVariant().Name,
				Variant: req.GetNameVariant().Variant,
				Type:    t,
			}
			ctx = logging.AttachRequestID(req.GetRequestId(), ctx, logger)
			loggerWithResource = logging.GetLoggerFromContext(ctx).WithResource(id.Type.ToLoggingResourceType(), id.Name, id.Variant)
		default:
			logger.Errorw("Invalid Stream for Get", "type", fmt.Sprintf("%T", casted))
			return fferr.NewInternalError(fmt.Errorf("invalid Stream for Get: %T", casted))
		}
		loggerWithResource.Debug("Looking up Resource")
		resource, err := serv.lookup.Lookup(ctx, id)
		if err != nil {
			loggerWithResource.Errorw("Unable to look up resource", "error", err)
			return err
		}
		loggerWithResource.Debug("Sending Resource")
		serialized := resource.Proto()
		if err := send(serialized); err != nil {
			loggerWithResource.Errorw("Error sending resource", "error", err)
			return fferr.NewInternalError(err)
		}
		loggerWithResource.Debug("Send Complete")
	}
}

func (serv *MetadataServer) genericList(ctx context.Context, t ResourceType, send sendFn) error {
	logger := logging.GetLoggerFromContext(ctx)
	logger.Infow("Listing Resources", "type", t)
	resources, err := serv.lookup.ListForType(t)
	if err != nil {
		logger.Error("Unable to lookup list for type %v: %v", t, err)
		return err
	}
	for _, res := range resources {
		loggerWithResource := logger.WithResource(t.ToLoggingResourceType(), res.ID().Name, res.ID().Variant)
		loggerWithResource.Debug("Getting %v", t)
		serialized := res.Proto()
		if err := send(serialized); err != nil {
			loggerWithResource.Errorw("Error sending resource", "error", err)
			return fferr.NewInternalError(err)
		}
	}
	return nil
}

// Resource Variant structs for Dashboard
type TrainingSetVariantResource struct {
	Created     time.Time                           `json:"created"`
	Description string                              `json:"description"`
	Name        string                              `json:"name"`
	Owner       string                              `json:"owner"`
	Provider    string                              `json:"provider"`
	Variant     string                              `json:"variant"`
	Label       NameVariant                         `json:"label"`
	Features    map[string][]FeatureVariantResource `json:"features"`
	Status      string                              `json:"status"`
	Error       string                              `json:"error"`
	Tags        Tags                                `json:"tags"`
	Properties  Properties                          `json:"properties"`
}

type FeatureVariantResource struct {
	Created     time.Time `json:"created"`
	Description string    `json:"description"`
	Entity      string    `json:"entity"`
	Name        string    `json:"name"`
	Owner       string    `json:"owner"`
	Provider    string    `json:"provider"`
	// TODO(simba) Make this not a string
	DataType     string                                  `json:"data-type"`
	Variant      string                                  `json:"variant"`
	Status       string                                  `json:"status"`
	Error        string                                  `json:"error"`
	Location     map[string]string                       `json:"location"`
	Source       NameVariant                             `json:"source"`
	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
	Tags         Tags                                    `json:"tags"`
	Properties   Properties                              `json:"properties"`
	Mode         string                                  `json:"mode"`
	IsOnDemand   bool                                    `json:"is-on-demand"`
	Definition   string                                  `json:"definition"`
}

type LabelVariantResource struct {
	Created      time.Time                               `json:"created"`
	Description  string                                  `json:"description"`
	Entity       string                                  `json:"entity"`
	Name         string                                  `json:"name"`
	Owner        string                                  `json:"owner"`
	Provider     string                                  `json:"provider"`
	DataType     string                                  `json:"data-type"`
	Variant      string                                  `json:"variant"`
	Location     map[string]string                       `json:"location"`
	Source       NameVariant                             `json:"source"`
	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
	Status       string                                  `json:"status"`
	Error        string                                  `json:"error"`
	Tags         Tags                                    `json:"tags"`
	Properties   Properties                              `json:"properties"`
}

type SourceVariantResource struct {
	Name           string                                  `json:"name"`
	Variant        string                                  `json:"variant"`
	Definition     string                                  `json:"definition"`
	Owner          string                                  `json:"owner"`
	Description    string                                  `json:"description"`
	Provider       string                                  `json:"provider"`
	Created        time.Time                               `json:"created"`
	Status         string                                  `json:"status"`
	Table          string                                  `json:"table"`
	TrainingSets   map[string][]TrainingSetVariantResource `json:"training-sets"`
	Features       map[string][]FeatureVariantResource     `json:"features"`
	Labels         map[string][]LabelVariantResource       `json:"labels"`
	LastUpdated    time.Time                               `json:"lastUpdated"`
	Schedule       string                                  `json:"schedule"`
	Tags           Tags                                    `json:"tags"`
	Properties     Properties                              `json:"properties"`
	SourceType     string                                  `json:"source-type"`
	Error          string                                  `json:"error"`
	Specifications map[string]string                       `json:"specifications"`
	Inputs         []NameVariant                           `json:"inputs"`
}

func getSourceString(variant *SourceVariant) string {
	if variant.IsSQLTransformation() {
		return variant.SQLTransformationQuery()
	} else if variant.IsDFTransformation() {
		return variant.DFTransformationQuerySource()
	} else {
		return variant.PrimaryDataSQLTableName()
	}
}

func getSourceType(variant *SourceVariant) string {
	if variant.IsSQLTransformation() {
		return "SQL Transformation"
	} else if variant.IsDFTransformation() {
		return "Dataframe Transformation"
	} else {
		return "Primary Table"
	}
}

func getSourceArgs(variant *SourceVariant) map[string]string {
	if variant.HasKubernetesArgs() {
		return variant.TransformationArgs().Format()
	}
	return map[string]string{}
}

func SourceShallowMap(variant *SourceVariant) SourceVariantResource {
	return SourceVariantResource{
		Name:           variant.Name(),
		Variant:        variant.Variant(),
		Definition:     getSourceString(variant),
		Owner:          variant.Owner(),
		Description:    variant.Description(),
		Provider:       variant.Provider(),
		Created:        variant.Created(),
		Status:         variant.Status().String(),
		LastUpdated:    variant.LastUpdated(),
		Schedule:       variant.Schedule(),
		Tags:           variant.Tags(),
		SourceType:     getSourceType(variant),
		Properties:     variant.Properties(),
		Error:          variant.Error(),
		Specifications: getSourceArgs(variant),
	}
}
