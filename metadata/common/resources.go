package common

import (
	"fmt"
	"github.com/featureform/logging"
	pb "github.com/featureform/metadata/proto"
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

var parentMapping = map[ResourceType]ResourceType{
	FEATURE_VARIANT:      FEATURE,
	LABEL_VARIANT:        LABEL,
	SOURCE_VARIANT:       SOURCE,
	TRAINING_SET_VARIANT: TRAINING_SET,
}

type ResourceID struct {
	Name    string
	Variant string
	Type    ResourceType
}

func (id ResourceID) Proto() *pb.ResourceID {
	return &pb.ResourceID{
		Resource: &pb.NameVariant{
			Name:    id.Name,
			Variant: id.Variant,
		},
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

func (id ResourceID) ToKey() string {
	return fmt.Sprintf("%s__%s__%s", id.Type, id.Name, id.Variant)
}
