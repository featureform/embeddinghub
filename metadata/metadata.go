// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

// TODO: need to figure out entry point for GetStatus
// modify if status is not in proto then check the task manager

package metadata

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/featureform/metadata/common"
	"github.com/featureform/storage/query"

	"github.com/featureform/metadata/equivalence"

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/featureform/filestore"
	"github.com/featureform/helpers/interceptors"
	"github.com/featureform/logging"
	"github.com/featureform/scheduling"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers/notifications"
	schproto "github.com/featureform/scheduling/proto"

	"github.com/pkg/errors"

	"golang.org/x/exp/slices"

	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/metadata/search"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	ptypes "github.com/featureform/provider/types"
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

// Returns an empty Resource Object of the given type to unmarshal etcd value into
func CreateEmptyResource(t ResourceType) (Resource, error) {
	switch t {
	case FEATURE:
		return &featureResource{&pb.Feature{}}, nil
	case FEATURE_VARIANT:
		return &featureVariantResource{&pb.FeatureVariant{}}, nil
	case LABEL:
		return &labelResource{&pb.Label{}}, nil
	case LABEL_VARIANT:
		return &labelVariantResource{&pb.LabelVariant{}}, nil
	case USER:
		return &userResource{&pb.User{}}, nil
	case ENTITY:
		return &entityResource{&pb.Entity{}}, nil
	case PROVIDER:
		return &providerResource{&pb.Provider{}}, nil
	case SOURCE:
		return &sourceResource{&pb.Source{}}, nil
	case SOURCE_VARIANT:
		return &sourceVariantResource{&pb.SourceVariant{}}, nil
	case TRAINING_SET:
		return &trainingSetResource{&pb.TrainingSet{}}, nil
	case TRAINING_SET_VARIANT:
		return &trainingSetVariantResource{&pb.TrainingSetVariant{}}, nil
	case MODEL:
		return &modelResource{&pb.Model{}}, nil
	default:
		return nil, fferr.NewUnimplementedErrorf("unable to create empty resource of type %T", t)
	}
}

type ComputationMode int32

const (
	PRECOMPUTED     ComputationMode = ComputationMode(pb.ComputationMode_PRECOMPUTED)
	CLIENT_COMPUTED                 = ComputationMode(pb.ComputationMode_CLIENT_COMPUTED)
	STREAMING                       = ComputationMode(pb.ComputationMode_STREAMING)
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
			return !CLIENT_COMPUTED.Equals(fv.serialized.Mode)
		}
	}
	return false
}

func (serv *MetadataServer) needsRun(ctx context.Context, res Resource) bool {
	logger := logging.GetLoggerFromContext(ctx)
	switch res.ID().Type {
	case TRAINING_SET_VARIANT:
		return true
	case FEATURE_VARIANT:
		fv, ok := res.(*featureVariantResource)
		if !ok {
			logger.Errorw("resource has type FEATURE VARIANT but failed to cast", "resource", res.ID())
			return false
		}
		// Don't need to run on-demand features
		isClientComputed := CLIENT_COMPUTED.Equals(fv.serialized.Mode)
		return !isClientComputed
	case SOURCE_VARIANT:
		sv, ok := res.(*sourceVariantResource)
		if !ok {
			logger.Errorw("resource has type SOURCE VARIANT but failed to cast", "resource", res.ID())
			return false
		}
		// Don't need to run primaries
		_, isTransform := sv.serialized.Definition.(*pb.SourceVariant_Transformation)
		return isTransform
	default:
		logger.Warnw("Calling needsRun on un-handled resource", "resource", res.ID())
		return false
	}
}

type ResourceID struct {
	Name    string
	Variant string
	Type    ResourceType
}

func (id ResourceID) NameVariantProto() *pb.NameVariant {
	return &pb.NameVariant{
		Name:    id.Name,
		Variant: id.Variant,
	}
}

func (id ResourceID) Proto() *pb.ResourceID {
	return &pb.ResourceID{
		Resource:     id.NameVariantProto(),
		ResourceType: id.Type.Serialized(),
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
	ID() ResourceID
	IsEquivalent(ResourceVariant) (bool, error)
	ToResourceVariantProto() *pb.ResourceVariant
	Owner() string
}

type Resource interface {
	Equals(any) bool
	Less(any) bool
	Notify(context.Context, ResourceLookup, operation, Resource) error
	ID() ResourceID
	Schedule() string
	Dependencies(context.Context, ResourceLookup) (ResourceLookup, error)
	Proto() proto.Message
	GetStatus() *pb.ResourceStatus
	UpdateStatus(*pb.ResourceStatus) error
	UpdateSchedule(string) error
	Update(ResourceLookup, Resource) error
}

func isDirectDependency(ctx context.Context, lookup ResourceLookup, dependency, parent Resource) (bool, error) {
	logger := logging.GetLoggerFromContext(ctx)
	logger.Debugw("checking direct dependency", "dependency", dependency.ID(), "parent", parent.ID())
	depId := dependency.ID()
	deps, depsErr := parent.Dependencies(ctx, lookup)
	if depsErr != nil {
		logger.Errorw("failed to get dependencies", "error", depsErr, "parent", parent.ID())
		return false, depsErr
	}
	return deps.Has(ctx, depId)
}

type DeletionMode int

const (
	ExcludeDeleted DeletionMode = iota // default: exclude deleted
	IncludeDeleted
	DeletedOnly
)

type ResourceLookupType string

type ResourceLookupOption interface {
	Type() ResourceLookupType
}

const DeleteLookupOptionType ResourceLookupType = "Deletion"

type DeleteLookupOption struct {
	deletionMode DeletionMode
}

func (opt DeleteLookupOption) Type() ResourceLookupType {
	return DeleteLookupOptionType
}

type ResourceLookupOptions struct {
	deletionMode DeletionMode
}

func NewResourceLookupOptions(opts ...ResourceLookupOption) (ResourceLookupOptions, error) {
	ro := ResourceLookupOptions{
		deletionMode: ExcludeDeleted, // default
	}

	deletionOptionSet := false

	for _, opt := range opts {
		switch opt.Type() {

		case DeleteLookupOptionType:
			// If we already saw a DeleteLookupOption, that's an error:
			if deletionOptionSet {
				return ro, fferr.NewInternalErrorf("multiple DeletionResourceLookupType options")
			}
			deletionOptionSet = true

			dlo, ok := opt.(DeleteLookupOption)
			if !ok {
				return ro, fferr.NewInternalErrorf("failed to cast DeletionResourceLookupType option")
			}

			ro.deletionMode = dlo.deletionMode
		}
	}

	return ro, nil
}

func (opt ResourceLookupOptions) generateQueryOpts() []query.Query {
	var queryOpts []query.Query

	switch opt.deletionMode {
	case ExcludeDeleted:
		// Only include non-deleted resources (deleted timestamp is NULL)
		queryOpts = append(queryOpts, query.ValueEquals{
			Column: query.SQLColumn{Column: "marked_for_deletion_at"},
			Value:  nil,
		})
	case DeletedOnly:
		// Only include deleted resources (deleted timestamp is not NULL)
		queryOpts = append(queryOpts, query.ValueEquals{
			Not:    true,
			Column: query.SQLColumn{Column: "marked_for_deletion_at"},
			Value:  nil,
		})
	case IncludeDeleted:
		// No filter needed - include all resources regardless of deleted timestamp
	}

	return queryOpts
}

type ResourceLookup interface {
	Lookup(context.Context, ResourceID, ...ResourceLookupOption) (Resource, error)
	//Lookup(context.Context, ResourceID, ResourceLookupOpts) (Resource, error)
	Has(context.Context, ResourceID) (bool, error) // add is delete
	Set(context.Context, ResourceID, Resource) error
	Submap(context.Context, []ResourceID) (ResourceLookup, error)                                    // add is delete
	ListForType(context.Context, ResourceType) ([]Resource, error)                                   // add is delete
	List(context.Context) ([]Resource, error)                                                        // add is delete
	ListVariants(context.Context, ResourceType, string, ...ResourceLookupOption) ([]Resource, error) // add is delete
	HasJob(context.Context, ResourceID) (bool, error)
	SetJob(context.Context, ResourceID, string) error
	SetStatus(context.Context, ResourceID, *pb.ResourceStatus) error
	SetSchedule(context.Context, ResourceID, string) error
	Delete(context.Context, ResourceID) error
}

type resourceStatusImplementation interface {
	// TODO we have a few ways to save a status, consolidate and clean up the abstractions
	SetAndSaveStatus(ctx context.Context, status *scheduling.Status, msg string, lookup ResourceLookup) error
}

type resourceTaskImplementation interface {
	TaskIDs() ([]scheduling.TaskID, error)
}

type SearchWrapper struct {
	Searcher search.Searcher
	ResourceLookup
}

func (wrapper SearchWrapper) Set(ctx context.Context, id ResourceID, res Resource) error {
	if err := wrapper.ResourceLookup.Set(ctx, id, res); err != nil {
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

func (lookup LocalResourceLookup) Lookup(ctx context.Context, id ResourceID, opts ...ResourceLookupOption) (Resource, error) {
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

func (lookup LocalResourceLookup) Has(ctx context.Context, id ResourceID) (bool, error) {
	_, has := lookup[id]
	return has, nil
}

func (lookup LocalResourceLookup) Set(ctx context.Context, id ResourceID, res Resource) error {
	lookup[id] = res
	return nil
}

func (lookup LocalResourceLookup) Submap(ctx context.Context, ids []ResourceID) (ResourceLookup, error) {
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

func (lookup LocalResourceLookup) ListForType(ctx context.Context, t ResourceType) ([]Resource, error) {
	resources := make([]Resource, 0)
	for id, res := range lookup {
		if id.Type == t {
			resources = append(resources, res)
		}
	}
	return resources, nil
}

func (lookup LocalResourceLookup) ListVariants(ctx context.Context, t ResourceType, name string, opts ...ResourceLookupOption) ([]Resource, error) {
	resources := make([]Resource, 0)
	for id, res := range lookup {
		if id.Type == t && id.Name == name {
			resources = append(resources, res)
		}
	}
	return resources, nil
}

func (lookup LocalResourceLookup) List(ctx context.Context) ([]Resource, error) {
	resources := make([]Resource, 0, len(lookup))
	for _, res := range lookup {
		resources = append(resources, res)
	}
	return resources, nil
}

func (lookup LocalResourceLookup) SetStatus(ctx context.Context, id ResourceID, status *pb.ResourceStatus) error {
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

func (lookup LocalResourceLookup) SetJob(ctx context.Context, id ResourceID, schedule string) error {
	return nil
}

func (lookup LocalResourceLookup) SetSchedule(ctx context.Context, id ResourceID, schedule string) error {
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

func (lookup LocalResourceLookup) HasJob(ctx context.Context, id ResourceID) (bool, error) {
	return false, nil
}

func (lookup LocalResourceLookup) Delete(ctx context.Context, id ResourceID) error {
	delete(lookup, id)
	return nil
}

type sourceResource struct {
	serialized *pb.Source
}

func (resource *sourceResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: SOURCE,
	}
}

func (resource *sourceResource) Equals(other any) bool {
	if _, ok := other.(*sourceResource); !ok {
		return false
	}
	return resource == other
}

func (resource *sourceResource) Less(other any) bool {
	if _, ok := other.(*sourceResource); !ok {
		return false
	}
	return resource.ID().String() < other.(Resource).ID().String()
}

func (resource *sourceResource) Schedule() string {
	return ""
}

func (resource *sourceResource) Dependencies(ctx context.Context, lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *sourceResource) Proto() proto.Message {
	return resource.serialized
}

func (this *sourceResource) Notify(ctx context.Context, lookup ResourceLookup, op operation, that Resource) error {
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

func (resource *sourceResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *sourceResource) UpdateStatus(status *pb.ResourceStatus) error {
	resource.serialized.Status = status
	return nil
}

func (resource *sourceResource) UpdateSchedule(schedule string) error {
	return fferr.NewInternalError(fmt.Errorf("not implemented"))
}

func (resource *sourceResource) Update(lookup ResourceLookup, updateRes Resource) error {
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

func (resource *sourceVariantResource) Equals(other any) bool {
	if _, ok := other.(*sourceVariantResource); !ok {
		return false
	}
	return resource == other
}

func (resource *sourceVariantResource) Less(other any) bool {
	if _, ok := other.(*sourceVariantResource); !ok {
		return false
	}
	return resource.ID().String() < other.(Resource).ID().String()
}

func (resource *sourceVariantResource) Schedule() string {
	return resource.serialized.Schedule
}

func (resource *sourceVariantResource) Dependencies(ctx context.Context, lookup ResourceLookup) (ResourceLookup, error) {
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
	deps, err := lookup.Submap(ctx, depIds)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (resource *sourceVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (sourceVariantResource *sourceVariantResource) Notify(ctx context.Context, lookup ResourceLookup, op operation, that Resource) error {
	id := that.ID()
	t := id.Type
	key := id.NameVariantProto()
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

func (resource *sourceVariantResource) UpdateStatus(status *pb.ResourceStatus) error {
	resource.serialized.LastUpdated = tspb.Now()
	resource.serialized.Status = status
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
	otherCasted, ok := other.(*sourceVariantResource)
	if !ok {
		return false, nil
	}

	thisSv, err := equivalence.SourceVariantFromProto(resource.serialized)
	if err != nil {
		return false, err
	}

	otherSv, err := equivalence.SourceVariantFromProto(otherCasted.serialized)
	if err != nil {
		return false, err
	}

	return thisSv.IsEquivalent(otherSv), nil
}

func (resource *sourceVariantResource) SetAndSaveStatus(ctx context.Context, status *scheduling.Status, msg string, lookup ResourceLookup) error {
	resource.serialized.Status.Status = status.Proto()
	resource.serialized.Status.ErrorMessage = msg
	err := lookup.Set(ctx, resource.ID(), resource)
	if err != nil {
		return err
	}
	return nil
}

func (resource *sourceVariantResource) TaskIDs() ([]scheduling.TaskID, error) {
	// Check if using a deprecated taskID singleton
	if resource.serialized.TaskId != "" {
		return parseResourceTasks([]string{resource.serialized.TaskId})
	}
	return parseResourceTasks(resource.serialized.TaskIdList)
}

func (resource *sourceVariantResource) ToResourceVariantProto() *pb.ResourceVariant {
	return &pb.ResourceVariant{Resource: &pb.ResourceVariant_SourceVariant{SourceVariant: resource.serialized}}
}

func (resource *sourceVariantResource) Owner() string {
	return resource.serialized.Owner
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

func (resource *featureResource) Equals(other any) bool {
	if _, ok := other.(*featureResource); !ok {
		return false
	}
	return resource == other
}

func (resource *featureResource) Less(other any) bool {
	if _, ok := other.(*featureResource); !ok {
		return false
	}
	return resource.ID().String() < other.(Resource).ID().String()
}

func (resource *featureResource) Schedule() string {
	return ""
}

func (resource *featureResource) Dependencies(ctx context.Context, lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *featureResource) Proto() proto.Message {
	return resource.serialized
}

func (this *featureResource) Notify(ctx context.Context, lookup ResourceLookup, op operation, that Resource) error {
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

func (resource *featureResource) UpdateStatus(status *pb.ResourceStatus) error {
	resource.serialized.Status = status
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

func (resource *featureVariantResource) Equals(other any) bool {
	if _, ok := other.(*featureVariantResource); !ok {
		return false
	}
	return resource == other
}

func (resource *featureVariantResource) Less(other any) bool {
	if _, ok := other.(*featureVariantResource); !ok {
		return false
	}
	return resource.ID().String() < other.(Resource).ID().String()
}

func (resource *featureVariantResource) Schedule() string {
	return resource.serialized.Schedule
}

func (resource *featureVariantResource) Dependencies(ctx context.Context, lookup ResourceLookup) (ResourceLookup, error) {
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
		depIds = append(depIds,
			ResourceID{
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
	deps, err := lookup.Submap(ctx, depIds)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (resource *featureVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *featureVariantResource) Notify(ctx context.Context, lookup ResourceLookup, op operation, that Resource) error {
	if !PRECOMPUTED.Equals(this.serialized.Mode) {
		return nil
	}
	id := that.ID()
	relevantOp := op == create_op && id.Type == TRAINING_SET_VARIANT
	if !relevantOp {
		return nil
	}
	key := id.NameVariantProto()
	this.serialized.Trainingsets = append(this.serialized.Trainingsets, key)
	return nil
}

func (resource *featureVariantResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *featureVariantResource) UpdateStatus(status *pb.ResourceStatus) error {
	resource.serialized.LastUpdated = tspb.Now()
	resource.serialized.Status = status
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
	otherCasted, ok := other.(*featureVariantResource)
	if !ok {
		return false, nil
	}

	thisFv, err := equivalence.FeatureVariantFromProto(resource.serialized)
	if err != nil {
		return false, err
	}
	otherFv, err := equivalence.FeatureVariantFromProto(otherCasted.serialized)
	if err != nil {
		return false, err
	}

	return thisFv.IsEquivalent(otherFv), nil
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

func (resource *featureVariantResource) SetAndSaveStatus(ctx context.Context, status *scheduling.Status, msg string, lookup ResourceLookup) error {
	resource.serialized.Status.Status = status.Proto()
	resource.serialized.Status.ErrorMessage = msg
	err := lookup.Set(ctx, resource.ID(), resource)
	if err != nil {
		return err
	}
	return nil
}

func (resource *featureVariantResource) TaskIDs() ([]scheduling.TaskID, error) {
	// Check if using a deprecated taskID singleton
	if resource.serialized.TaskId != "" {
		return parseResourceTasks([]string{resource.serialized.TaskId})
	}
	return parseResourceTasks(resource.serialized.TaskIdList)
}

func (resource *featureVariantResource) Owner() string {
	return resource.serialized.Owner
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

func (resource *labelResource) Equals(other any) bool {
	if _, ok := other.(*labelResource); !ok {
		return false
	}
	return resource == other
}

func (resource *labelResource) Less(other any) bool {
	if _, ok := other.(*labelResource); !ok {
		return false
	}
	return resource.ID().String() < other.(Resource).ID().String()
}

func (resource *labelResource) Schedule() string {
	return ""
}

func (resource *labelResource) Dependencies(ctx context.Context, lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *labelResource) Proto() proto.Message {
	return resource.serialized
}

func (this *labelResource) Notify(ctx context.Context, lookup ResourceLookup, op operation, that Resource) error {
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

func (resource *labelResource) UpdateStatus(status *pb.ResourceStatus) error {
	resource.serialized.Status = status
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

func (resource *labelVariantResource) Equals(other any) bool {
	if _, ok := other.(*labelVariantResource); !ok {
		return false
	}
	return resource == other
}

func (resource *labelVariantResource) Less(other any) bool {
	if _, ok := other.(*labelVariantResource); !ok {
		return false
	}
	return resource.ID().String() < other.(Resource).ID().String()
}

func (resource *labelVariantResource) Schedule() string {
	return ""
}

func (resource *labelVariantResource) Dependencies(ctx context.Context, lookup ResourceLookup) (ResourceLookup, error) {
	serialized := resource.serialized
	// dependencies for a label variant are different depending on the computation mode
	depIds := []ResourceID{
		{
			Name: serialized.Owner,
			Type: USER,
		},
		{
			Name: serialized.Name,
			Type: LABEL,
		},
		{
			Name: serialized.Provider,
			Type: PROVIDER,
		},
		{
			Name: serialized.Entity,
			Type: ENTITY,
		},
		{
			Name: serialized.Provider,
			Type: PROVIDER,
		},
	}
	isStream := serialized.GetStream() != nil
	if !isStream {
		depIds = append(
			depIds, ResourceID{
				Name:    serialized.Source.Name,
				Variant: serialized.Source.Variant,
				Type:    SOURCE_VARIANT,
			},
		)
	}
	deps, err := lookup.Submap(ctx, depIds)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (resource *labelVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *labelVariantResource) Notify(ctx context.Context, lookup ResourceLookup, op operation, that Resource) error {
	isStream := this.serialized.GetStream() != nil
	if isStream {
		return nil
	}
	id := that.ID()
	releventOp := op == create_op && id.Type == TRAINING_SET_VARIANT
	if !releventOp {
		return nil
	}
	key := id.NameVariantProto()
	this.serialized.Trainingsets = append(this.serialized.Trainingsets, key)
	return nil
}

func (resource *labelVariantResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *labelVariantResource) UpdateStatus(status *pb.ResourceStatus) error {
	resource.serialized.Status = status
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

func (resource *labelVariantResource) SetAndSaveStatus(ctx context.Context, status *scheduling.Status, msg string, lookup ResourceLookup) error {
	resource.serialized.Status.Status = status.Proto()
	resource.serialized.Status.ErrorMessage = msg
	err := lookup.Set(ctx, resource.ID(), resource)
	if err != nil {
		return err
	}
	return nil
}

func (resource *labelVariantResource) TaskIDs() ([]scheduling.TaskID, error) {
	// Check if using a deprecated taskID singleton
	if resource.serialized.TaskId != "" {
		return parseResourceTasks([]string{resource.serialized.TaskId})
	}
	return parseResourceTasks(resource.serialized.TaskIdList)
}

func (resource *labelVariantResource) IsEquivalent(other ResourceVariant) (bool, error) {
	otherCasted, ok := other.(*labelVariantResource)
	if !ok {
		return false, nil
	}

	thisLv, err := equivalence.LabelVariantFromProto(resource.serialized)
	if err != nil {
		return false, err
	}
	otherLv, err := equivalence.LabelVariantFromProto(otherCasted.serialized)
	if err != nil {
		return false, err
	}

	return thisLv.IsEquivalent(otherLv), nil
}

func (resource *labelVariantResource) ToResourceVariantProto() *pb.ResourceVariant {
	return &pb.ResourceVariant{Resource: &pb.ResourceVariant_LabelVariant{LabelVariant: resource.serialized}}
}

func (resource *labelVariantResource) Owner() string {
	return resource.serialized.Owner
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

func (resource *trainingSetResource) Equals(other any) bool {
	if _, ok := other.(*trainingSetResource); !ok {
		return false
	}
	return resource == other
}

func (resource *trainingSetResource) Less(other any) bool {
	if _, ok := other.(*trainingSetResource); !ok {
		return false
	}
	return resource.ID().String() < other.(Resource).ID().String()
}

func (resource *trainingSetResource) Schedule() string {
	return ""
}

func (resource *trainingSetResource) Dependencies(ctx context.Context, lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *trainingSetResource) Proto() proto.Message {
	return resource.serialized
}

func (this *trainingSetResource) Notify(ctx context.Context, lookup ResourceLookup, op operation, that Resource) error {
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

func (resource *trainingSetResource) UpdateStatus(status *pb.ResourceStatus) error {
	resource.serialized.Status = status
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

func (resource *trainingSetVariantResource) Equals(other any) bool {
	if _, ok := other.(*trainingSetVariantResource); !ok {
		return false
	}
	return resource == other
}

func (resource *trainingSetVariantResource) Less(other any) bool {
	if _, ok := other.(*trainingSetVariantResource); !ok {
		return false
	}
	return resource.ID().String() < other.(Resource).ID().String()
}

func (resource *trainingSetVariantResource) Schedule() string {
	return resource.serialized.Schedule
}

func (resource *trainingSetVariantResource) Dependencies(ctx context.Context, lookup ResourceLookup) (ResourceLookup, error) {
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
	deps, err := lookup.Submap(ctx, depIds)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (resource *trainingSetVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *trainingSetVariantResource) Notify(ctx context.Context, lookup ResourceLookup, op operation, that Resource) error {
	return nil
}

func (resource *trainingSetVariantResource) GetStatus() *pb.ResourceStatus {
	return resource.serialized.GetStatus()
}

func (resource *trainingSetVariantResource) UpdateStatus(status *pb.ResourceStatus) error {
	resource.serialized.LastUpdated = tspb.Now()
	resource.serialized.Status = status
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

func (resource *trainingSetVariantResource) SetAndSaveStatus(ctx context.Context, status *scheduling.Status, msg string, lookup ResourceLookup) error {
	resource.serialized.Status.Status = status.Proto()
	resource.serialized.Status.ErrorMessage = msg
	err := lookup.Set(ctx, resource.ID(), resource)
	if err != nil {
		return err
	}
	return nil
}

func (resource *trainingSetVariantResource) TaskIDs() ([]scheduling.TaskID, error) {
	// Check if using a deprecated taskID singleton
	if resource.serialized.TaskId != "" {
		return parseResourceTasks([]string{resource.serialized.TaskId})
	}
	return parseResourceTasks(resource.serialized.TaskIdList)
}

func (resource *trainingSetVariantResource) IsEquivalent(other ResourceVariant) (bool, error) {
	otherCasted, ok := other.(*trainingSetVariantResource)
	if !ok {
		return false, nil
	}

	thisTsv, err := equivalence.TrainingSetVariantFromProto(resource.serialized)
	if err != nil {
		return false, err
	}
	otherTsv, err := equivalence.TrainingSetVariantFromProto(otherCasted.serialized)
	if err != nil {
		return false, err
	}

	return thisTsv.IsEquivalent(otherTsv), nil
}

func (resource *trainingSetVariantResource) ToResourceVariantProto() *pb.ResourceVariant {
	return &pb.ResourceVariant{Resource: &pb.ResourceVariant_TrainingSetVariant{TrainingSetVariant: resource.serialized}}
}

func (resource *trainingSetVariantResource) Owner() string {
	return resource.serialized.Owner
}

func (resource *trainingSetVariantResource) Validate(ctx context.Context, lookup ResourceLookup) error {
	resId := ResourceID{Name: resource.serialized.Label.Name, Variant: resource.serialized.Label.Variant, Type: LABEL_VARIANT}
	label, err := lookup.Lookup(ctx, resId)
	if err != nil {
		return err
	}
	labelVariant, isLabelVariant := label.(*labelVariantResource)
	if !isLabelVariant {
		return fferr.NewDatasetNotFoundError(resource.ID().Name, resource.ID().Variant, fmt.Errorf("label variant not found"))
	}
	entityMap := map[string]struct{}{labelVariant.serialized.Entity: {}}
	for _, feature := range resource.serialized.Features {
		fvResId := ResourceID{Name: feature.Name, Variant: feature.Variant, Type: FEATURE_VARIANT}
		featureResource, err := lookup.Lookup(ctx, fvResId)
		if err != nil {
			return err
		}
		featureVariant, isFeatureVariant := featureResource.(*featureVariantResource)
		if !isFeatureVariant {
			return fferr.NewDatasetNotFoundError(feature.Name, feature.Variant, fmt.Errorf("feature variant not found"))
		}
		switch featureVariant.serialized.Mode {
		case pb.ComputationMode_PRECOMPUTED:
			if _, exists := entityMap[featureVariant.serialized.Entity]; !exists {
				return fferr.NewInvalidArgumentErrorf("feature %s entity %s does not match label entity %s", feature.Name, featureVariant.serialized.Entity, labelVariant.serialized.Entity)
			}
		case pb.ComputationMode_CLIENT_COMPUTED:
			return fferr.NewInvalidArgumentErrorf("feature %s has unsupported computation mode %s", feature.Name, featureVariant.serialized.Mode)
		default:
			return fferr.NewInternalErrorf("feature %s has unknown computation mode %s", feature.Name, featureVariant.serialized.Mode)
		}
	}
	return nil
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

func (resource *modelResource) Equals(other any) bool {
	if _, ok := other.(*modelResource); !ok {
		return false
	}
	return resource == other
}

func (resource *modelResource) Less(other any) bool {
	if _, ok := other.(*modelResource); !ok {
		return false
	}
	return resource.ID().String() < other.(Resource).ID().String()
}

func (resource *modelResource) Schedule() string {
	return ""
}

func (resource *modelResource) Dependencies(ctx context.Context, lookup ResourceLookup) (ResourceLookup, error) {
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
	deps, err := lookup.Submap(ctx, depIds)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (resource *modelResource) Proto() proto.Message {
	return resource.serialized
}

func (this *modelResource) Notify(ctx context.Context, lookup ResourceLookup, op operation, that Resource) error {
	return nil
}

func (resource *modelResource) GetStatus() *pb.ResourceStatus {
	return &pb.ResourceStatus{Status: pb.ResourceStatus_NO_STATUS}
}

func (resource *modelResource) UpdateStatus(status *pb.ResourceStatus) error {
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

func (resource *userResource) Equals(other any) bool {
	if _, ok := other.(*userResource); !ok {
		return false
	}
	return resource == other
}

func (resource *userResource) Less(other any) bool {
	if _, ok := other.(*userResource); !ok {
		return false
	}
	return resource.ID().String() < other.(Resource).ID().String()
}

func (resource *userResource) Schedule() string {
	return ""
}

func (resource *userResource) Dependencies(ctx context.Context, lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *userResource) Proto() proto.Message {
	return resource.serialized
}

func (this *userResource) Notify(ctx context.Context, lookup ResourceLookup, op operation, that Resource) error {
	if isDep, err := isDirectDependency(ctx, lookup, this, that); err != nil {
		return err
	} else if !isDep {
		return nil
	}
	id := that.ID()
	key := id.NameVariantProto()
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

func (resource *userResource) UpdateStatus(status *pb.ResourceStatus) error {
	resource.serialized.Status = status
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

func (resource *providerResource) Equals(other any) bool {
	if _, ok := other.(*providerResource); !ok {
		return false
	}
	return resource == other
}

func (resource *providerResource) Less(other any) bool {
	if _, ok := other.(*providerResource); !ok {
		return false
	}
	return resource.ID().String() < other.(Resource).ID().String()
}

func (resource *providerResource) Schedule() string {
	return ""
}

func (resource *providerResource) Dependencies(ctx context.Context, lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *providerResource) Proto() proto.Message {
	return resource.serialized
}

func (this *providerResource) Notify(ctx context.Context, lookup ResourceLookup, op operation, that Resource) error {
	if isDep, err := isDirectDependency(ctx, lookup, this, that); err != nil {
		return err
	} else if !isDep {
		return nil
	}
	id := that.ID()
	key := id.NameVariantProto()
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

func (resource *providerResource) UpdateStatus(status *pb.ResourceStatus) error {
	resource.serialized.Status = status
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

func (resource *entityResource) Equals(other any) bool {
	if _, ok := other.(*entityResource); !ok {
		return false
	}
	return resource == other
}

func (resource *entityResource) Less(other any) bool {
	if _, ok := other.(*entityResource); !ok {
		return false
	}
	return resource.ID().String() < other.(Resource).ID().String()
}

func (resource *entityResource) Schedule() string {
	return ""
}

func (resource *entityResource) Dependencies(ctx context.Context, lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *entityResource) Proto() proto.Message {
	return resource.serialized
}

func (this *entityResource) Notify(ctx context.Context, lookup ResourceLookup, op operation, that Resource) error {
	id := that.ID()
	key := id.NameVariantProto()
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

func (resource *entityResource) UpdateStatus(status *pb.ResourceStatus) error {
	resource.serialized.Status = status
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
	Logger      logging.Logger
	lookup      ResourceLookup
	address     string
	grpcServer  *grpc.Server
	listener    net.Listener
	taskManager *scheduling.TaskMetadataManager
	pb.UnimplementedMetadataServer
	schproto.UnimplementedTasksServer
	slackNotifier       notifications.SlackNotifier
	resourcesRepository ResourcesRepository
}

func (serv *MetadataServer) CreateTaskRun(ctx context.Context, request *schproto.CreateRunRequest) (*schproto.RunID, error) {
	_, _, logger := serv.Logger.InitializeRequestID(ctx)
	tid, err := scheduling.ParseTaskID(request.GetTaskID().GetId())
	if err != nil {
		logger.Errorw("failed to parse task id", "task id", request.GetTaskID().GetId(), "error", err)
		return nil, err
	}

	rid, err := serv.taskManager.CreateTaskRun(request.Name, tid, scheduling.OnApplyTrigger{TriggerName: "apply"})
	if err != nil {
		return nil, err
	}
	return &schproto.RunID{Id: rid.ID.String()}, nil
}

func (serv *MetadataServer) SyncUnfinishedRuns(ctx context.Context, empty *schproto.Empty) (*schproto.Empty, error) {
	err := serv.taskManager.SyncIncompleteRuns()
	if err != nil {
		return nil, err
	}
	return &schproto.Empty{}, nil
}

func NewMetadataServer(config *Config) (*MetadataServer, error) {
	config.Logger.Infow("Creating new metadata server", "Address:", config.Address)
	baseLookup := MemoryResourceLookup{config.TaskManager.Storage}
	wrappedLookup, err := initializeLookup(config, &baseLookup, search.NewMeilisearch)
	if err != nil {
		return nil, err
	}
	resourcesRepo, err := NewResourcesRepositoryFromLookup(&baseLookup)
	if err != nil {
		return nil, err
	}

	return &MetadataServer{
		lookup:              wrappedLookup,
		address:             config.Address,
		Logger:              config.Logger,
		taskManager:         &config.TaskManager,
		resourcesRepository: resourcesRepo,
		slackNotifier:       *notifications.NewSlackNotifier(os.Getenv("SLACK_CHANNEL_ID"), config.Logger),
	}, nil
}

func initializeLookup(config *Config, lookup *MemoryResourceLookup, newSearchStub search.NewMeilisearchFunc) (ResourceLookup, error) {
	if config.SearchParams == nil {
		config.Logger.Debug("No configuration search params are present, using non-search wrappped lookup")
		return lookup, nil
	}
	searcher, err := newSearchStub(config.SearchParams)
	if err != nil {
		return nil, err
	}

	return &SearchWrapper{
		Searcher:       searcher,
		ResourceLookup: lookup,
	}, nil
}

func (serv *MetadataServer) GetTaskByID(ctx context.Context, taskID *schproto.TaskID) (*schproto.TaskMetadata, error) {
	_, _, logger := serv.Logger.InitializeRequestID(ctx)
	tid, err := scheduling.ParseTaskID(taskID.GetId())
	if err != nil {
		logger.Errorw("failed to parse task id", "task id", taskID.GetId(), "error", err)
		return nil, err
	}
	task, err := serv.taskManager.GetTaskByID(tid)
	if err != nil {
		logger.Errorw("failed to get task by id", "task id", taskID.GetId(), "error", err)
		return nil, err
	}
	p, err := task.ToProto()
	if err != nil {
		logger.Errorw("failed to wrap task metadata", "error", err)
		return nil, err
	}
	return p, nil
}

func (serv *MetadataServer) GetRuns(id *schproto.TaskID, stream schproto.Tasks_GetRunsServer) error {
	tid, err := scheduling.ParseTaskID(id.GetId())
	if err != nil {
		return err
	}
	runs, err := serv.taskManager.GetTaskRunMetadata(tid)
	if err != nil {
		return err
	}
	for _, run := range runs {
		wrapped, err := run.ToProto()
		if err != nil {
			return err
		}
		err = stream.Send(wrapped)
		if err != nil {
			return err
		}
	}
	return nil
}

func (serv *MetadataServer) GetAllRuns(_ *schproto.Empty, stream schproto.Tasks_GetAllRunsServer) error {
	runs, err := serv.taskManager.GetAllTaskRuns()
	if err != nil {
		return err
	}

	for _, run := range runs {
		wrapped, err := run.ToProto()
		if err != nil {
			return err
		}
		err = stream.Send(wrapped)
		if err != nil {
			return err
		}
	}

	return nil
}

func (serv *MetadataServer) GetUnfinishedRuns(_ *schproto.Empty, stream schproto.Tasks_GetUnfinishedRunsServer) error {
	runs, err := serv.taskManager.GetUnfinishedTaskRuns()
	if err != nil {
		return err
	}

	for _, run := range runs {
		wrapped, err := run.ToProto()
		if err != nil {
			return err
		}
		err = stream.Send(wrapped)
		if err != nil {
			return err
		}
	}

	return nil
}

func (serv *MetadataServer) GetRunMetadata(ctx context.Context, id *schproto.TaskRunID) (*schproto.TaskRunMetadata, error) {
	_, _, logger := serv.Logger.InitializeRequestID(ctx)
	tid, err := scheduling.ParseTaskID(id.TaskID.GetId())
	if err != nil {
		logger.Errorw("failed to parse task id", "task id", id.TaskID.GetId(), "error", err)
		return nil, err
	}
	rid, err := scheduling.ParseTaskRunID(id.RunID.GetId())
	if err != nil {
		logger.Errorw("failed to parse run id", "run id", id.RunID.GetId(), "error", err)
		return nil, err
	}
	run, err := serv.taskManager.GetRunByID(tid, rid)
	if err != nil {
		logger.Errorw("failed to get run by id", "task id", id.TaskID.GetId(), "run id", id.RunID.GetId(), "error", err)
		return nil, err
	}
	wrapped, err := run.ToProto()
	if err != nil {
		logger.Errorw("failed to wrap task run metadata", "error", err)
		return nil, err
	}
	return wrapped, nil

}

func (serv *MetadataServer) GetLatestRun(ctx context.Context, taskID *schproto.TaskID) (*schproto.TaskRunMetadata, error) {
	_, _, logger := serv.Logger.InitializeRequestID(ctx)
	tid, err := scheduling.ParseTaskID(taskID.GetId())
	if err != nil {
		logger.Errorw("failed to parse task id", "task id", taskID.GetId(), "error", err)
		return nil, err
	}
	run, err := serv.taskManager.GetLatestRun(tid)
	if err != nil {
		logger.Errorw("failed to get latest run", "task id", taskID.GetId(), "error", err)
		return nil, err
	}

	wrapped, err := run.ToProto()
	if err != nil {
		logger.Errorw("failed to wrap task run metadata", "error", err)
		return nil, err
	}

	return wrapped, nil
}

func (serv *MetadataServer) SetRunStatus(ctx context.Context, update *schproto.StatusUpdate) (*schproto.Empty, error) {
	_, _, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Debugw("SetRunStatus", "status", update.Status)
	rid, err := scheduling.ParseTaskRunID(update.GetRunID().GetId())
	if err != nil {
		logger.Errorw("failed to parse run id", "run id", update.GetRunID().GetId(), "error", err)
		return nil, err
	}
	tid, err := scheduling.ParseTaskID(update.GetTaskID().GetId())
	if err != nil {
		logger.Errorw("failed to parse task id", "task id", update.GetTaskID().GetId(), "error", err)
		return nil, err
	}

	err = serv.taskManager.SetRunStatus(rid, tid, update.Status)
	if err != nil {
		logger.Errorw("failed to set run status", "run id", update.GetRunID().GetId(), "task id", update.GetTaskID().GetId(), "error", err)
		return nil, err
	}
	return &schproto.Empty{}, nil
}

func (serv *MetadataServer) AddRunLog(ctx context.Context, log *schproto.Log) (*schproto.Empty, error) {
	_, _, logger := serv.Logger.InitializeRequestID(ctx)
	rid, err := scheduling.ParseTaskRunID(log.GetRunID().GetId())
	if err != nil {
		logger.Errorw("failed to parse run id", "run id", log.GetRunID().GetId(), "error", err)
		return nil, err
	}
	tid, err := scheduling.ParseTaskID(log.GetTaskID().GetId())
	if err != nil {
		logger.Errorw("failed to parse task id", "task id", log.GetTaskID().GetId(), "error", err)
		return nil, err
	}
	err = serv.taskManager.AppendRunLog(rid, tid, log.Log)
	if err != nil {
		logger.Errorw("failed to append run log", "run id", log.GetRunID().GetId(), "task id", log.GetTaskID().GetId(), "error", err)
		return nil, err
	}
	return &schproto.Empty{}, nil
}

func (serv *MetadataServer) SetRunResumeID(ctx context.Context, update *schproto.ResumeIDUpdate) (*schproto.Empty, error) {
	_, _, logger := serv.Logger.InitializeRequestID(ctx)
	taskID, runID, resumeID := update.GetTaskID().GetId(), update.GetRunID().GetId(), update.GetResumeID().GetId()
	logger = logger.WithValues(map[string]interface{}{
		"task_id":   taskID,
		"run_id":    runID,
		"resume_id": resumeID,
	})
	logger.Info("Setting Resume ID")
	tid, err := scheduling.ParseTaskID(taskID)
	if err != nil {
		logger.Errorw("failed to parse task id", "error", err)
		return nil, err
	}
	rid, err := scheduling.ParseTaskRunID(runID)
	if err != nil {
		logger.Errorw("failed to parse run id", "error", err)
		return nil, err
	}
	err = serv.taskManager.SetResumeID(rid, tid, ptypes.ResumeID(resumeID))
	if err != nil {
		logger.Errorw("failed to set resume ID", "error", err)
		return nil, err
	}
	return &schproto.Empty{}, nil
}

func (serv *MetadataServer) WatchForCancel(ctx context.Context, id *schproto.TaskRunID) (*pb.ResourceStatus, error) {
	_, _, logger := serv.Logger.InitializeRequestID(ctx)
	tid, err := scheduling.ParseTaskID(id.TaskID.GetId())
	if err != nil {
		logger.Errorw("failed to parse task id", "task id", id.TaskID.GetId(), "error", err)
		return nil, err
	}
	rid, err := scheduling.ParseTaskRunID(id.RunID.GetId())
	if err != nil {
		logger.Errorw("failed to parse run id", "run id", id.RunID.GetId(), "error", err)
		return nil, err
	}
	err = serv.taskManager.WatchForCancel(tid, rid)
	if err != nil {
		return nil, err
	}
	return &pb.ResourceStatus{Status: pb.ResourceStatus_CANCELLED}, nil
}

func (serv *MetadataServer) SetRunEndTime(ctx context.Context, update *schproto.RunEndTimeUpdate) (*schproto.Empty, error) {
	_, _, logger := serv.Logger.InitializeRequestID(ctx)
	taskID, runID := update.GetTaskID().GetId(), update.GetRunID().GetId()
	logger = logger.WithValues(map[string]interface{}{
		"task_id": taskID,
		"run_id":  runID,
	})
	logger.Info("Setting Run End Time")
	tid, err := scheduling.ParseTaskID(taskID)
	if err != nil {
		logger.Errorw("failed to parse task id", "error", err)
		return nil, err
	}
	rid, err := scheduling.ParseTaskRunID(runID)
	if err != nil {
		logger.Errorw("failed to parse run id", "error", err)
		return nil, err
	}
	err = serv.taskManager.SetRunEndTime(rid, tid, update.End.AsTime())
	if err != nil {
		logger.Errorw("failed to set run end time", "error", err)
		return nil, err
	}
	return &schproto.Empty{}, nil
}

func (serv *MetadataServer) Serve() error {
	if serv.grpcServer != nil {
		return fferr.NewInternalErrorf("server already running")
	}
	lis, err := net.Listen("tcp", serv.address)
	if err != nil {
		return fferr.NewInternalErrorf("cannot listen to server address %s", serv.address)
	}
	return serv.ServeOnListener(lis)
}

func (serv *MetadataServer) ServeOnListener(lis net.Listener) error {
	serv.listener = lis
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(interceptors.UnaryServerErrorInterceptor), grpc.StreamInterceptor(interceptors.StreamServerErrorInterceptor))
	pb.RegisterMetadataServer(grpcServer, serv)
	schproto.RegisterTasksServer(grpcServer, serv)
	serv.grpcServer = grpcServer
	serv.Logger.Infow("Server starting", "Address", serv.listener.Addr().String())
	return grpcServer.Serve(lis)
}

func (serv *MetadataServer) GracefulStop() error {
	if serv.grpcServer == nil {
		return fferr.NewInternalErrorf("server not running")
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
	Logger       logging.Logger
	SearchParams *search.MeilisearchParams
	TaskManager  scheduling.TaskMetadataManager
	Address      string
}

func (serv *MetadataServer) RequestScheduleChange(ctx context.Context, req *pb.ScheduleChangeRequest) (*pb.Empty, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Requesting schedule change", "resource_id", req.ResourceId, "schedule", req.Schedule)
	resID := ResourceID{Name: req.ResourceId.Resource.Name, Variant: req.ResourceId.Resource.Variant, Type: ResourceType(req.ResourceId.ResourceType)}
	err := serv.lookup.SetSchedule(ctx, resID, req.Schedule)
	return &pb.Empty{}, err
}

func (serv *MetadataServer) SetResourceStatus(ctx context.Context, req *pb.SetStatusRequest) (*pb.Empty, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Setting resource status", "resource_id", req.ResourceId, "status", req.Status.Status)
	resID := ResourceID{Name: req.ResourceId.Resource.Name, Variant: req.ResourceId.Resource.Variant, Type: ResourceType(req.ResourceId.ResourceType)}
	err := serv.lookup.SetStatus(ctx, resID, req.Status)
	if err != nil {
		logger.Errorw("Could not set resource status", "error", err.Error())
	} else {
		//if no error, notify slack
		go func() {
			slackError := serv.slackNotifier.ChangeNotification(
				resID.Type.String(),
				resID.Name,
				resID.Variant,
				req.Status.String(),
				req.Status.ErrorMessage,
			)

			if slackError != nil {
				logger.Errorw("Could not notify slack for resource udpate", "error", slackError.Error())
			}
		}()
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
	taskTarget := scheduling.NameVariant{Name: variant.Name, Variant: variant.Variant, ResourceType: FEATURE_VARIANT.String()}
	task, err := serv.taskManager.CreateTask("mytask", scheduling.ResourceCreation, taskTarget)
	if err != nil {
		return nil, err
	}
	variant.TaskIdList = []string{task.ID.String()}
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

func (serv *MetadataServer) MarkForDeletion(ctx context.Context, request *pb.MarkForDeletionRequest) (*pb.MarkForDeletionResponse, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Deleting resource", "resource_id", request.ResourceId)

	if serv.resourcesRepository.Type() == ResourcesRepositoryTypeMemory {
		logger.Infow("Deletion not supported for memory repository")
		return &pb.MarkForDeletionResponse{}, fferr.NewInternalErrorf("Deletion not supported for memory repository")
	}

	resId := common.ResourceID{Name: request.ResourceId.Resource.Name, Variant: request.ResourceId.Resource.Variant, Type: common.ResourceType(request.ResourceId.ResourceType)}
	notCommonResId := ResourceID{Name: resId.Name, Variant: resId.Variant, Type: ResourceType(resId.Type)}

	resource, err := serv.lookup.Lookup(ctx, notCommonResId)
	if err != nil {
		logger.Errorw("Could not find resource to delete", "error", err.Error())
		return &pb.MarkForDeletionResponse{}, err
	}

	//status, err := serv.getStatusFromTasks(ctx, resource)
	//if err != nil {
	//	logger.Errorw("Could not get status from tasks", "error", err.Error())
	//	return &pb.MarkForDeletionResponse{}, err
	//}

	//if status != pb.ResourceStatus_READY && status != pb.ResourceStatus_CREATED {
	//	logger.Errorw("Resource is not ready for deletion", "status", status)
	//	return &pb.MarkForDeletionResponse{}, fferr.NewInternalErrorf("Resource is not ready for deletion")
	//}

	isDeletableErr := serv.isDeletable(ctx, resource)
	if isDeletableErr != nil {
		logger.Errorw("Could not delete resource", "error", isDeletableErr.Error())
		return &pb.MarkForDeletionResponse{}, isDeletableErr
	}

	deleteErr := serv.resourcesRepository.MarkForDeletion(ctx, resId)
	if deleteErr != nil {
		logger.Errorw("Could not delete resource", "error", deleteErr.Error())
		return &pb.MarkForDeletionResponse{}, deleteErr
	}

	if serv.needsJob(resource) {
		taskTarget := scheduling.NameVariant{Name: resId.Name, Variant: resId.Variant, ResourceType: resId.Type.String()}
		task, err := serv.taskManager.CreateTask("delete-task", scheduling.ResourceDeletion, taskTarget)
		if err != nil {
			return nil, err
		}

		needsJobList := []common.ResourceType{
			common.FEATURE_VARIANT,
			common.LABEL_VARIANT,
			common.TRAINING_SET_VARIANT,
			common.SOURCE_VARIANT,
		}
		if slices.Contains(needsJobList, resId.Type) {
			taskId := task.ID
			logger.Info("Creating Job", resId.String())
			trigger := scheduling.OnApplyTrigger{TriggerName: "Apply"}
			taskName := fmt.Sprintf("Deleting Resource %s", resId.String())
			taskRun, createTaskErr := serv.taskManager.CreateTaskRun(taskName, taskId, trigger)
			if createTaskErr != nil {
				logger.Errorw("unable to create task run", "task name", taskName, "task ID", taskId, "trigger", trigger.TriggerName, "error", createTaskErr)
				return nil, createTaskErr
			}
			logger.Infow("Successfully Created Task", "task ID", taskRun.TaskId, "taskrun ID", taskRun.ID, "resource ID", resId.String())
		}
		return &pb.MarkForDeletionResponse{}, nil
	} else {
		err = serv.lookup.Delete(ctx, notCommonResId)
		if err != nil {
			logger.Errorw("Could not delete resource", "error", err.Error())
			return &pb.MarkForDeletionResponse{}, err
		}
	}

	return &pb.MarkForDeletionResponse{}, nil
}

func (serv *MetadataServer) GetStagedForDeletionResource(ctx context.Context, request *pb.GetStagedForDeletionResourceRequest) (*pb.GetStagedForDeletionResourceResponse, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource(logging.ResourceType(request.ResourceId.ResourceType), request.ResourceId.Resource.Name, request.ResourceId.Resource.Variant)
	logger.Infow("Getting staged for deletion resource")

	resId := ResourceID{Name: request.ResourceId.Resource.Name, Variant: request.ResourceId.Resource.Variant, Type: ResourceType(request.ResourceId.ResourceType)}

	resourceLookupOpt := DeleteLookupOption{DeletedOnly}
	resource, err := serv.lookup.Lookup(ctx, resId, resourceLookupOpt)
	if err != nil {
		logger.Errorw("Could not find resource to delete", "error", err.Error())
		return &pb.GetStagedForDeletionResourceResponse{}, err
	}

	logger.Debugw("Found resource to delete", "resource", resource)
	var rv *pb.ResourceVariant
	switch resId.Type {
	case FEATURE_VARIANT:
		featureVariant := resource.(*featureVariantResource)
		rv = &pb.ResourceVariant{
			Resource: &pb.ResourceVariant_FeatureVariant{
				FeatureVariant: featureVariant.serialized,
			},
		}
	case LABEL_VARIANT:
		labelVariant := resource.(*labelVariantResource)
		rv = &pb.ResourceVariant{
			Resource: &pb.ResourceVariant_LabelVariant{
				LabelVariant: labelVariant.serialized,
			},
		}
	case SOURCE_VARIANT:
		sourceVariant := resource.(*sourceVariantResource)
		rv = &pb.ResourceVariant{
			Resource: &pb.ResourceVariant_SourceVariant{
				SourceVariant: sourceVariant.serialized,
			},
		}
	case TRAINING_SET_VARIANT:
		trainingSetVariant := resource.(*trainingSetVariantResource)
		rv = &pb.ResourceVariant{
			Resource: &pb.ResourceVariant_TrainingSetVariant{
				TrainingSetVariant: trainingSetVariant.serialized,
			},
		}
	default:
		return nil, fferr.NewInternalErrorf("Resource type %s is not deletable", resId.Type)
	}

	return &pb.GetStagedForDeletionResourceResponse{ResourceVariant: rv}, nil
}

func (serv *MetadataServer) isDeletable(ctx context.Context, resource Resource) error {
	// we can only delete snowflake resource variants

	// cast resource to source variant
	resourcesAllowedForDeletion := []ResourceType{
		FEATURE_VARIANT,
		LABEL_VARIANT,
		SOURCE_VARIANT,
		TRAINING_SET_VARIANT,
		PROVIDER,
	}

	if !slices.Contains(resourcesAllowedForDeletion, resource.ID().Type) {
		return fferr.NewInternalErrorf("Resource type %s is not deletable", resource.ID().Type)
	}

	switch r := resource.(type) {
	case *trainingSetVariantResource:
		return serv.validateTrainingSetDeletion(ctx, r)
	case *sourceVariantResource:
		return serv.validateSourceDeletion(ctx, r)
	case *featureVariantResource:
		return serv.validateFeatureDeletion(ctx, r)
	case *labelVariantResource:
		return serv.validateLabelDeletion(ctx, r)
	}
	return nil
}

func (serv *MetadataServer) validateProviderType(ctx context.Context, providerName string) error {
	provider, err := serv.lookup.Lookup(ctx, ResourceID{Name: providerName, Type: PROVIDER})
	if err != nil {
		return err
	}

	providerResource := provider.(*providerResource)
	t := providerResource.serialized.Type
	serv.Logger.Debugw("Provider type", "type", t)

	// we can only delete snowflake provider for now
	if t != pt.SnowflakeOffline.String() {
		return fferr.NewInternalErrorf("Resource is not deletable because it is not a snowflake provider")
	}
	return nil
}

func (serv *MetadataServer) validateTrainingSetDeletion(ctx context.Context, ts *trainingSetVariantResource) error {
	wrapped := WrapProtoTrainingSetVariant(ts.serialized)
	serv.Logger.Debugw("Check provider for deletion", "provider", wrapped.Provider())
	return serv.validateProviderType(ctx, wrapped.Provider())
}

func (serv *MetadataServer) validateSourceDeletion(ctx context.Context, sv *sourceVariantResource) error {
	wrapped := WrapProtoSourceVariant(sv.serialized)
	serv.Logger.Debugw("Check provider for deletion", "provider", wrapped.Provider())

	if err := serv.validateProviderType(ctx, wrapped.Provider()); err != nil {
		return err
	}

	return nil
}

func (serv *MetadataServer) validateFeatureDeletion(ctx context.Context, fv *featureVariantResource) error {
	wrapped := WrapProtoFeatureVariant(fv.serialized)
	serv.Logger.Debugw("Check provider for deletion", "provider", wrapped.Provider())
	return serv.validateProviderType(ctx, wrapped.Provider())
}

func (serv *MetadataServer) validateLabelDeletion(ctx context.Context, lv *labelVariantResource) error {
	wrapped := WrapProtoLabelVariant(lv.serialized)
	serv.Logger.Debugw("Check provider for deletion", "provider", wrapped.Provider())
	return serv.validateProviderType(ctx, wrapped.Provider())
}

func (serv *MetadataServer) FinalizeDeletion(ctx context.Context, request *pb.FinalizeDeletionRequest) (*pb.FinalizeDeletionResponse, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Finalizing resource deletion", "resource_id", request.ResourceId)
	resId := ResourceID{Name: request.ResourceId.Resource.Name, Variant: request.ResourceId.Resource.Variant, Type: ResourceType(request.ResourceId.ResourceType)}

	err := serv.lookup.Delete(ctx, resId)
	if err != nil {
		return nil, err
	}

	return &pb.FinalizeDeletionResponse{}, nil
}

func (serv *MetadataServer) GetFeatures(stream pb.Metadata_GetFeaturesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Info("Opened Get Features stream")
	return serv.genericGet(ctx, stream, FEATURE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Feature))
	})
}

func (serv *MetadataServer) GetFeatureVariants(stream pb.Metadata_GetFeatureVariantsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Info("Opened Get Feature Variants stream")
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
	taskTarget := scheduling.NameVariant{Name: variant.Name, Variant: variant.Variant, ResourceType: LABEL_VARIANT.String()}
	task, err := serv.taskManager.CreateTask("mytask", scheduling.ResourceCreation, taskTarget)
	if err != nil {
		logger.Errorw("Failed to create task", "error", err)
		return nil, err
	}
	variant.TaskIdList = []string{task.ID.String()}
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
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Info("Opened Get Labels stream")
	return serv.genericGet(ctx, stream, LABEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Label))
	})
}

func (serv *MetadataServer) GetLabelVariants(stream pb.Metadata_GetLabelVariantsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Info("Opened Get Label Variants stream")
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
	tsRes := &trainingSetVariantResource{variant}
	if err := tsRes.Validate(ctx, serv.lookup); err != nil {
		return nil, err
	}
	variant.Created = tspb.New(time.Now())
	taskTarget := scheduling.NameVariant{Name: variant.Name, Variant: variant.Variant, ResourceType: TRAINING_SET_VARIANT.String()}
	task, err := serv.taskManager.CreateTask("mytask", scheduling.ResourceCreation, taskTarget)
	if err != nil {
		return nil, err
	}
	variant.TaskIdList = []string{task.ID.String()}

	return serv.genericCreate(ctx, tsRes, func(name, variant string) Resource {
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
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Info("Opened Get Training Sets stream")
	return serv.genericGet(ctx, stream, TRAINING_SET, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.TrainingSet))
	})
}

func (serv *MetadataServer) GetTrainingSetVariants(stream pb.Metadata_GetTrainingSetVariantsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Info("Opened Get Training Set Variants stream")
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
	logger.Info("Creating Source Variant", "source_name", variantRequest.SourceVariant.Name, "source_variant", variantRequest.SourceVariant.Variant)

	variant := variantRequest.SourceVariant
	variant.Created = tspb.New(time.Now())
	taskTarget := scheduling.NameVariant{Name: variant.Name, Variant: variant.Variant, ResourceType: SOURCE_VARIANT.String()}
	task, err := serv.taskManager.CreateTask("mytask", scheduling.ResourceCreation, taskTarget)
	if err != nil {
		logger.Errorw("failed to create task", "error", err)
		return nil, err
	}
	variant.TaskIdList = []string{task.ID.String()}
	if _, isTransformation := variant.Definition.(*pb.SourceVariant_Transformation); isTransformation {
		if err := serv.addTransformationLocation(ctx, variant); err != nil {
			logger.Errorw("failed to add transformation location", "error", err)
			return nil, err
		}
	}
	return serv.genericCreate(ctx, &sourceVariantResource{variant}, func(name, variant string) Resource {
		return &sourceResource{
			&pb.Source{
				Name:           name,
				DefaultVariant: variant,
				// This will be set when the change is propagated to dependencies.
				Variants: []string{},
			},
		}
	})
}

// addTransformationLocation adds the location to the transformation field of the source variant given the client
// isn't capable of populating the location field properly. Given metadata is the source of truth and "upstream"
func (serv *MetadataServer) addTransformationLocation(ctx context.Context, sv *pb.SourceVariant) error {
	logger := serv.Logger.With("source_name", sv.Name, "source_variant", sv.Variant)
	if sv.GetTransformation() == nil {
		logger.Warn("Transformation not set on source variant")
		return nil
	}
	if sv.GetTransformation().Location != nil {
		logger.Debug("Transformation location already set")
		return nil
	}
	providerName := sv.GetProvider()
	logger.Debugw("Looking up provider for transformation location", "provider", providerName)
	providerResource, err := serv.lookup.Lookup(ctx, ResourceID{Name: providerName, Type: PROVIDER})
	if err != nil {
		logger.Errorw("Failed to lookup provider resource", "provider", providerName, "error", err)
		return err
	}
	logger.Debugw("Provider for transformation location", "provider", providerName)
	providerProto, isProviderProto := providerResource.Proto().(*pb.Provider)
	if !isProviderProto {
		logger.Errorw("Provider resource is not a provider proto", "provider", providerResource.Proto())
		return fferr.NewInternalErrorf("provider resource is not a provider proto")
	}
	localizer, err := GetLocalizer(pt.Type(providerProto.GetType()), providerProto.GetSerializedConfig())
	if err != nil {
		logger.Errorw("Failed to get localizer", "provider", providerName, "error", err)
		return err
	}
	location, err := localizer.Localize(sv)
	if err != nil {
		logger.Errorw("Failed to localize transformation location", "provider", providerName, "error", err)
		return err
	}
	logger.Debugw("Localized transformation location", "location", location.Location(), "type", location.Type())
	transformation := sv.GetTransformation()

	switch lt := location.(type) {
	case *pl.SQLLocation:
		logger.Debugw("Adding SQL location to transformation", "location", lt.Location())
		transformation.Location = &pb.Transformation_Table{
			Table: &pb.SQLTable{
				Name: lt.Location(),
			},
		}
	case *pl.CatalogLocation:
		logger.Debugw("Adding catalog location to transformation", "database", lt.Database(), "table", lt.Table())
		transformation.Location = &pb.Transformation_Catalog{
			Catalog: &pb.CatalogTable{
				Database:    lt.Database(),
				Table:       lt.Table(),
				TableFormat: lt.TableFormat(),
			},
		}
	case *pl.FileStoreLocation:
		logger.Debugw("Adding filestore location to transformation", "path", lt.Filepath().ToURI())
		transformation.Location = &pb.Transformation_Filestore{
			Filestore: &pb.FileStoreTable{
				Path: lt.Filepath().ToURI(),
			},
		}
	default:
		logger.Errorw("Unknown location type", "type", lt)
		return fferr.NewInternalErrorf("unknown location type: %s", lt)
	}

	return nil
}

func (serv *MetadataServer) GetSources(stream pb.Metadata_GetSourcesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Info("Opened Get Sources stream")
	return serv.genericGet(ctx, stream, SOURCE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Source))
	})
}

func (serv *MetadataServer) GetSourceVariants(stream pb.Metadata_GetSourceVariantsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Info("Opened Get Source Variants stream")
	return serv.genericGet(ctx, stream, SOURCE_VARIANT, func(msg proto.Message) error {
		sv, isSourceVariant := msg.(*pb.SourceVariant)
		if !isSourceVariant {
			return fferr.NewInternalErrorf("expected source variant, got %T", msg)
		}
		if err := serv.sourceVariantBackwardsCompatibility(ctx, sv); err != nil {
			return err
		}
		return stream.Send(sv)
	})
}

// sourceVariantBackwardsCompatibility is an update operation that supports backwards compatibility for source variants that
// were created before the expansion of location type into `CatalogTable` and `FileStoreTable` (i.e. PR #1109); for primary
// sources, it achieves this by checking the location "name" to see if it stars with `glue://`, for Iceberg catalog locations,
// or `s3://` or `s3a://`, for S3 filestore locations and updating the source variant's location to the new format.
func (serv *MetadataServer) sourceVariantBackwardsCompatibility(ctx context.Context, sv *pb.SourceVariant) error {
	logger := serv.Logger.With("source_name", sv.Name, "source_variant", sv.Variant)
	logger.Debug("Checking source variant for backwards compatibility")
	switch def := sv.GetDefinition().(type) {
	case *pb.SourceVariant_PrimaryData:
		logger.Debug("Backfilling primary data location")
		location := def.PrimaryData.GetLocation()

		sqlTable, isSQLTable := location.(*pb.PrimaryData_Table)
		// If the location is not a SQL table, the assumption is that it has already been backfilled
		// or is a resource created using the new protos; in either case, nothing needs to be done.
		if !isSQLTable {
			logger.Debugw("Primary data location is not a SQL table", "location", location)
			return nil
		}

		name := sqlTable.Table.GetName()

		isCatalogTable := strings.HasPrefix(name, "glue://")
		isFilestoreTable := strings.HasPrefix(name, filestore.S3Prefix) || strings.HasPrefix(name, filestore.S3APrefix)

		// If the location name doesn't match the expected prefixes, it is assumed that the location is already
		// correctly defined as a SQL table location.
		if !isCatalogTable && !isFilestoreTable {
			logger.Debugw("Primary data location is either newly created or has already been backfilled", "location_name", name)
			return nil
		}

		if isCatalogTable {
			logger.Debug("Primary data location is a catalog table")
			// We no longer need this faux scheme to differentiate between catalog and filestore locations
			glueLocation := strings.TrimPrefix(name, "glue://")
			// The remainder of the "name" will be the database and table name separated by a "/" as if
			// they were part of a file path.
			parts := strings.Split(glueLocation, "/")
			if len(parts) != 2 {
				logger.Errorw("Invalid glue location", "location", glueLocation)
				return fferr.NewInternalErrorf("invalid glue location: %s; expected 'database/table'", glueLocation)
			}
			logger.Debugw("Backfilling primary data location as catalog table", "database", parts[0], "table", parts[1])
			catalogSv, isPrimaryData := sv.GetDefinition().(*pb.SourceVariant_PrimaryData)
			if !isPrimaryData {
				return fferr.NewInternalErrorf("source variant definition is not primary data; got %T", sv.GetDefinition())
			}

			catalogSv.PrimaryData.Location = &pb.PrimaryData_Catalog{
				Catalog: &pb.CatalogTable{
					Database: parts[0],
					Table:    parts[1],
					// Given Iceberg was the only supported table format prior to the expansion of location types,
					// we can safely assume that the table format is Iceberg.
					TableFormat: string(pc.Iceberg),
				},
			}
		} else if isFilestoreTable {
			logger.Debugw("Backfilling primary data location as filestore table", "path", name)
			filestoreSv, isPrimaryData := sv.GetDefinition().(*pb.SourceVariant_PrimaryData)
			if !isPrimaryData {
				return fferr.NewInternalErrorf("source variant definition is not primary data; got %T", sv.GetDefinition())
			}

			filestoreSv.PrimaryData.Location = &pb.PrimaryData_Filestore{
				Filestore: &pb.FileStoreTable{
					Path: name,
				},
			}
		}
	case *pb.SourceVariant_Transformation:
		logger.Debug("Backfilling transformation location")
		// addTransformationLocation already works to handle new transformations, so we just need to call it here
		// on existing transformations to ensure they are correctly backfilled.
		if err := serv.addTransformationLocation(ctx, sv); err != nil {
			return err
		}
	default:
		return fferr.NewInternalErrorf("unknown source variant definition type: %T", def)
	}
	id := ResourceID{Name: sv.Name, Variant: sv.Variant, Type: SOURCE_VARIANT}
	logger.Debugw("Setting source variant", "id", id)
	if err := serv.lookup.Set(ctx, id, &sourceVariantResource{sv}); err != nil {
		logger.Errorw("Failed to set source variant", "id", id, "error", err)
		return err
	}
	logger.Debug("Successfully backfilled location for source variant")
	return nil
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
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Info("Opened Get Users stream")
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
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Info("Opened Get Providers stream")
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
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Info("Opened Get Entities stream")
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
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Info("Opened Get Models stream")
	return serv.genericGet(ctx, stream, MODEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Model))
	})
}

// Run updates resources that have already been applied.
func (serv *MetadataServer) Run(ctx context.Context, req *pb.RunRequest) (*pb.Empty, error) {
	ctx = logging.AttachRequestID(req.RequestId, ctx, serv.Logger)
	baseLogger := logging.GetLoggerFromContext(ctx)
	baseLogger.Infow("Running resource variants", "num", len(req.Variants))
	for _, variant := range req.Variants {
		resVar, _, err := serv.extractResourceVariant(variant)
		if err != nil {
			baseLogger.Errorw("Failed to extract resource variant", "res", resVar, "error", err)
			return nil, err
		}
		id := resVar.ID()
		logger := baseLogger.WithResource(id.Type.ToLoggingResourceType(), id.Name, id.Variant)
		// What we receive from the client won't have Task IDs set, so we grab directly from lookup again.
		logger.Debugw("Looking up resource", "res", id)
		res, err := serv.lookup.Lookup(ctx, id)
		if err != nil {
			logger.Errorw("Failed to lookup resource", "error", err)
			return nil, err
		}
		if taskImpl, hasTasks := res.(resourceTaskImplementation); serv.needsRun(ctx, res) && hasTasks {
			if err := serv.createTaskRuns(ctx, id, taskImpl, logger); err != nil {
				return nil, err
			}
		} else {
			logger.Infow("Resource doesn't need run", "res", res.ID())
		}
	}
	return &pb.Empty{}, nil
}

func (serv *MetadataServer) createTaskRuns(ctx context.Context, id ResourceID, taskImpl resourceTaskImplementation, logger logging.Logger) error {
	logger.Infow("Creating TaskRun for resource")
	taskIDs, err := taskImpl.TaskIDs()
	if err != nil {
		logger.Errorw("error getting task IDs from ResourceTaskIplementation", "error", err)
		return err
	}
	for _, taskId := range taskIDs {
		trigger := scheduling.OnApplyTrigger{TriggerName: "Run"}
		taskName := fmt.Sprintf("Create Resource %s (%s)", id.Name, id.Variant)
		// This creates task runs to be picked up by the coordinator.
		taskRun, err := serv.taskManager.CreateTaskRun(taskName, taskId, trigger)
		if err != nil {
			logger.Errorw("unable to create task run", "task name", taskName, "task ID", taskId, "trigger", trigger.TriggerName, "error", err)
			return err
		}
		logger.Infow("Successfully Created Task", "task ID", taskRun.TaskId, "taskrun ID", taskRun.ID)
	}
	return nil
}

// GetEquivalent attempts to find an equivalent resource based on the provided ResourceVariant.
func (serv *MetadataServer) GetEquivalent(ctx context.Context, req *pb.GetEquivalentRequest) (*pb.ResourceVariant, error) {
	ctx = logging.AttachRequestID(req.RequestId, ctx, serv.Logger)
	logging.GetLoggerFromContext(ctx).Infow("Getting Equivalent Resource Variant", "resource", req.Variant.Resource)
	// todox: need to decide how to handle username. won't be availlable in OSS
	return serv.getEquivalent(ctx, req, true, "default")
}

/*
*
This method is used to get the equivalent resource variant for a given resource variant. readyStatus is used to determine
if we should only return the equivalent resource variant if it is ready.
*/
func (serv *MetadataServer) getEquivalent(ctx context.Context, req *pb.GetEquivalentRequest, filterStatus bool, username string) (*pb.ResourceVariant, error) {
	noEquivalentResponse := &pb.ResourceVariant{}
	logger := logging.GetLoggerFromContext(ctx)
	logger.Infow("getEquivalent: START to get Equivalent Resource Variant", "request", req.String())
	currentResource, resType, err := serv.extractResourceVariant(req.Variant)

	if err != nil {
		logger.Errorw("error extracting resource variant", "resource variant", req, "error", err)
		return nil, err
	}
	logger.Debugw("getEquivalent: extracted resource variant")

	resourcesForType, err := serv.lookup.ListVariants(ctx, resType, currentResource.ID().Name)
	if err != nil {
		logger.Errorw("Unable to list resources", "error", err)
		return nil, err
	}

	if len(resourcesForType) == 0 {
		logger.Debugw("getEquivalent: no resources found for type", "type", resType)
		return nil, nil
	}
	logger.Debugw("getEquivalent: listed resource variants")
	filtered, err := serv.filterResources(ctx, resourcesForType, username, filterStatus)
	if err != nil {
		return nil, err
	}
	if len(filtered) == 0 {
		logger.Debugw("getEquivalent: no resources found for type after filtering", "type", resType)
		return nil, nil
	}
	logger.Debugw("getEquivalent: filtered resources")
	equivalentResourceVariant, err := serv.findEquivalent(ctx, filtered, currentResource)
	if err != nil {
		logger.Errorw("Unable to find equivalent resource", "error", err)
		return nil, err
	}
	logger.Debugw("getEquivalent: ran findEquivalent")

	if equivalentResourceVariant == nil {
		return noEquivalentResponse, nil
	}
	logger.Debugw("getEquivalent: Successfully finished get equivalent resource variant")
	return equivalentResourceVariant.ToResourceVariantProto(), nil
}

func (serv *MetadataServer) filterResources(ctx context.Context, resources []Resource, username string, filterStatus bool) ([]ResourceVariant, error) {
	logger := logging.GetLoggerFromContext(ctx)
	variants := make([]ResourceVariant, 0)

	// We need a slice of ResourceVariants to sort by Owner
	logger.Debugw("getEquivalent: filtering resources", "resource_count", len(resources))
	for _, res := range resources {
		logger.Infow("filtering resource", "resource", res.ID().String())
		// If we are filtering by ready status, we only want to return the equivalent resource variant if it is ready.
		if filterStatus && !isValidStatusForEquivalent(res) {
			continue
		}
		rv, ok := res.(ResourceVariant)
		if !ok {
			logger.Errorw("resource is not a ResourceVariant", "resource", res.ID().String())
			return nil, fferr.NewInvalidResourceTypeError(res.ID().Name, res.ID().Variant, fferr.ResourceType(res.ID().Type.String()), fmt.Errorf("resource is not a ResourceVariant: %T", res))
		}
		variants = append(variants, rv)
	}
	logger.Debugw("getEquivalent: filtered resources loop")

	// Sorting the variants by Owner ensures that a user's own resources are considered first when checking for equivalence. This ensures
	// that a user who has been granted access to a super admin's resources will still have their own resources considered first for equivalence.
	sort.Slice(variants, func(i, j int) bool {
		if variants[i].Owner() == username && variants[j].Owner() != username {
			return true
		}
		return false
	})
	logger.Debugw("getEquivalent: sorted resources")

	return variants, nil
}

// findEquivalent searches through a slice of Resources to find an equivalent ResourceVariant.
func (serv *MetadataServer) findEquivalent(ctx context.Context, resources []ResourceVariant, resource ResourceVariant) (ResourceVariant, error) {
	logger := logging.GetLoggerFromContext(ctx)
	for _, other := range resources {
		logger.Infow("finding equivalent", "this", resource.ID().String(), "other", other.ID().String())
		equivalent, err := resource.IsEquivalent(other)
		if err != nil {
			logger.Errorw("error checking equivalence", "error", err)
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

var equivalentStatuses = mapset.NewSet(
	pb.ResourceStatus_NO_STATUS,
	pb.ResourceStatus_READY,
	pb.ResourceStatus_PENDING,
	pb.ResourceStatus_RUNNING,
	pb.ResourceStatus_CREATED,
)

func isValidStatusForEquivalent(res Resource) bool {
	resourceStatus := res.GetStatus()
	return resourceStatus != nil && equivalentStatuses.Contains(resourceStatus.Status)
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

	// Create the parent first. Better to have a hanging parent than a hanging dependency.

	parentId, hasParent := id.Parent()
	if hasParent {
		parentExists, err := serv.lookup.Has(ctx, parentId)
		if err != nil {
			logger.Errorw("Unable to check if parent exists", "parent-id", parentId, "error", err)
			return nil, err
		}

		if !parentExists {
			logger.Debug("Parent does not exist, creating new parent")
			parent := init(id.Name, id.Variant)
			logger.Infow("Parent ID", "Parent ID", parent.ID().String())
			err = serv.lookup.Set(ctx, parentId, parent)
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

	if err := serv.lookup.Set(ctx, id, res); err != nil {
		logger.Errorw("Error setting resource to lookup", "error", err)
		return nil, err
	}

	if serv.needsJob(res) && existing == nil {
		logger.Info("Creating Job")
		var taskIDs []scheduling.TaskID
		if r, ok := res.(resourceTaskImplementation); ok {
			taskIDs, err = r.TaskIDs()
			if err != nil {
				logger.Errorw("error getting task IDs from ResourceTaskImplementation", "error", err)
				return nil, err
			}
		}

		// This logic is going to get removed. Currently exists to support the old coordinator logic.
		for _, id := range taskIDs {
			logger.Info("Creating Job", res.ID().Name, res.ID().Variant)
			trigger := scheduling.OnApplyTrigger{TriggerName: "Apply"}
			taskName := fmt.Sprintf("Create Resource %s (%s)", res.ID().Name, res.ID().Variant)
			taskRun, err := serv.taskManager.CreateTaskRun(taskName, id, trigger)
			if err != nil {
				logger.Errorw("unable to create task run", "task name", taskName, "task ID", id, "trigger", trigger.TriggerName, "error", err)
				return nil, err
			}
			logger.Infow("Successfully Created Task", "task ID", taskRun.TaskId, "taskrun ID", taskRun.ID, "resource ID", res.ID().String())
		}

	}
	if existing == nil {
		if err := serv.propagateChange(ctx, res); err != nil {
			logger.Errorw("Propagate change error", "error", err)
			return nil, err
		}
	}
	return &pb.Empty{}, nil
}

func (serv *MetadataServer) setDefaultVariant(ctx context.Context, id ResourceID, defaultVariant string) error {
	logger := logging.GetLoggerFromContext(ctx)
	parent, err := serv.lookup.Lookup(ctx, id)
	if err != nil {
		logger.Errorw("Unable to lookup parent", "parent-id", id, "error", err)
		return err
	}
	var parentResource Resource
	if resource, ok := parent.(*sourceResource); ok {
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
	logger.Debugw("set default variant for source", "source", parent.ID().Name, "source type", parent.ID().Type, "variant", defaultVariant)
	err = serv.lookup.Set(ctx, id, parentResource)
	if err != nil {
		logger.Errorw("unable to set default variant", "parent-id", id, "error", err)
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
	serv.Logger.Debugw("Checking resource variant equivalence", "newRes", newRes.Proto(), "existing", existing.Proto())

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

func (serv *MetadataServer) propagateChange(ctx context.Context, newRes Resource) error {
	logger := logging.GetLoggerFromContext(ctx)
	logger.Infow("Propagating change", "resource", newRes.ID().String())
	visited := make(map[ResourceID]struct{})
	// We have to make it a var so that the anonymous function can call itself.
	var propagateChange func(parent Resource, depth int) error
	propagateChange = func(parent Resource, depth int) error {
		if depth == 2 {
			logger.Debugw("Reached max depth", "depth", depth)
			return nil
		}
		deps, err := parent.Dependencies(ctx, serv.lookup)
		if err != nil {
			logger.Errorw("Unable to get dependencies", "error", err)
			return err
		}
		depList, err := deps.List(ctx)
		if err != nil {
			logger.Errorw("Unable to list dependencies", "error", err)
			return err
		}
		for _, res := range depList {
			id := res.ID()
			if _, has := visited[id]; has {
				continue
			}
			visited[id] = struct{}{}
			if err := res.Notify(ctx, serv.lookup, create_op, newRes); err != nil {
				logger.Errorw("unable to notify dependency", "error", err)
				return err
			}
			if err := serv.lookup.Set(ctx, res.ID(), res); err != nil {
				logger.Errorw("unable to set dependency", "error", err)
				return err
			}
			if err := propagateChange(res, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return propagateChange(newRes, 0)
}

func (serv *MetadataServer) fetchStatus(taskId scheduling.TaskID) (*scheduling.Status, string, error) {
	run, err := serv.taskManager.GetLatestRun(taskId)
	if err != nil {
		_, ok := err.(*fferr.NoRunsForTaskError)
		if ok {
			noStatus := scheduling.NO_STATUS
			return &noStatus, "", nil
		}
		return nil, "", err
	}
	return &run.Status, run.Error, nil
}

func parseResourceTasks(ids []string) ([]scheduling.TaskID, error) {
	var taskIDs []scheduling.TaskID
	for _, id := range ids {
		if tid, err := scheduling.ParseTaskID(id); err != nil {
			return []scheduling.TaskID{}, fferr.NewInternalErrorf("unable to convert %s to type TaskIDs", id)
		} else {
			taskIDs = append(taskIDs, tid)
		}
	}
	return taskIDs, nil
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
			if recvErr != nil {
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

		// Fetches the latest run for the task and returns it for the CLI status watcher.
		// Can improve on this by linking the request to a specific run but that requires
		// additional changes
		if serv.needsJob(resource) {
			if _, err := serv.getStatusFromTasks(ctx, resource); err != nil {
				return err
			}
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

func (serv *MetadataServer) getStatusFromTasks(ctx context.Context, resource Resource) (pb.ResourceStatus_Status, error) {
	if res, ok := resource.(resourceStatusImplementation); ok {
		taskID, err := resource.(resourceTaskImplementation).TaskIDs()
		if err != nil {
			return pb.ResourceStatus_NO_STATUS, err
		}
		if len(taskID) > 0 {
			// This logic gets the status of the latest task for a resource. Need to create
			// better logic around this later
			status, msg, err := serv.fetchStatus(taskID[len(taskID)-1])
			if err != nil {
				serv.Logger.Errorw("Failed to set status", "error", err)
				return pb.ResourceStatus_NO_STATUS, err
			}
			if err := res.SetAndSaveStatus(ctx, status, msg, serv.lookup); err != nil {
				return pb.ResourceStatus_NO_STATUS, err
			}
			return status.Proto(), err
		}
	}
	return resource.GetStatus().GetStatus(), nil
}

func (serv *MetadataServer) genericList(ctx context.Context, t ResourceType, send sendFn) error {
	logger := logging.GetLoggerFromContext(ctx)
	logger.Infow("Listing Resources", "type", t)
	resources, err := serv.lookup.ListForType(ctx, t)
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

func (serv *MetadataServer) GetResourceDAG(ctx context.Context, r Resource) (ResourceDAG, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	dag, err := NewResourceDAG(ctx, serv.lookup, r)
	if err != nil {
		logger.Errorw("Unable to create Resource DAG", "error", err)
		return ResourceDAG{}, err
	}

	return dag, nil
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
	Created      time.Time `json:"created"`
	Description  string    `json:"description"`
	Entity       string    `json:"entity"`
	Name         string    `json:"name"`
	Owner        string    `json:"owner"`
	Provider     string    `json:"provider"`
	ProviderType string    `json:"providerType"`
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
	ProviderType string                                  `json:"providerType"`
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

// TODO: Might need to modify this to add number of features and number of labels
type EntityResource struct {
	Name         string                                  `json:"name"`
	Type         string                                  `json:"type"`
	Description  string                                  `json:"description"`
	Features     map[string][]FeatureVariantResource     `json:"features"`
	Labels       map[string][]LabelVariantResource       `json:"labels"`
	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
	Status       string                                  `json:"status"`
	Tags         Tags                                    `json:"tags"`
	Properties   Properties                              `json:"properties"`
}

type ProviderResource struct {
	Name             string                                  `json:"name"`
	Description      string                                  `json:"description"`
	Type             string                                  `json:"type"`          // resource type: "Provider"
	ProviderType     string                                  `json:"provider-type"` // Online, Offline, etc.
	Software         string                                  `json:"software"`
	Team             string                                  `json:"team"`
	SerializedConfig []byte                                  `json:"serialized-config"`
	Sources          map[string][]SourceVariantResource      `json:"sources"`
	Features         map[string][]FeatureVariantResource     `json:"features"`
	Labels           map[string][]LabelVariantResource       `json:"labels"`
	TrainingSets     map[string][]TrainingSetVariantResource `json:"training-sets"`
	Status           string                                  `json:"status"`
	Error            string                                  `json:"error"`
	Tags             Tags                                    `json:"tags"`
	Properties       Properties                              `json:"properties"`
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
