// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package logging

import (
	"context"
	"encoding/json"
	"github.com/featureform/metadata/proto"
	"io"
	"sync"
	"testing"

	"github.com/featureform/config"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

type Logger struct {
	*zap.SugaredLogger
	id     RequestID
	values *sync.Map
}

type RequestID string
type contextKey string
type ResourceType string

const (
	SkipProviderType string = ""
	NoVariant        string = ""
)

const (
	RequestIDKey = contextKey("request-id")
	LoggerKey    = contextKey("logger")
)

const (
	Provider           ResourceType = "provider"
	User               ResourceType = "user"
	Feature            ResourceType = "feature"
	FeatureVariant     ResourceType = "feature-variant"
	Source             ResourceType = "source"
	SourceVariant      ResourceType = "source-variant"
	TrainingSet        ResourceType = "training-set"
	TrainingSetVariant ResourceType = "training-set-variant"
	Entity             ResourceType = "entity"
	Model              ResourceType = "model"
	Label              ResourceType = "label"
	LabelVariant       ResourceType = "label-variant"
)

func ResourceTypeFromProto(resourceType proto.ResourceType) ResourceType {
	switch resourceType {
	case proto.ResourceType_PROVIDER:
		return Provider
	case proto.ResourceType_USER:
		return User
	case proto.ResourceType_FEATURE:
		return Feature
	case proto.ResourceType_FEATURE_VARIANT:
		return FeatureVariant
	case proto.ResourceType_SOURCE:
		return Source
	case proto.ResourceType_SOURCE_VARIANT:
		return SourceVariant
	case proto.ResourceType_TRAINING_SET:
		return TrainingSet
	case proto.ResourceType_TRAINING_SET_VARIANT:
		return TrainingSetVariant
	case proto.ResourceType_ENTITY:
		return Entity
	case proto.ResourceType_MODEL:
		return Model
	case proto.ResourceType_LABEL:
		return Label
	case proto.ResourceType_LABEL_VARIANT:
		return LabelVariant
	default:
		return ""
	}
}

const (
	DebugLevel = "debug"
	InfoLevel  = "info"
)

var GlobalLogger Logger

func init() {
	GlobalLogger = NewLogger("global-logger")
}

func NewRequestID() RequestID {
	return RequestID(uuid.New().String())
}

func (r RequestID) String() string {
	return string(r)
}

func (logger Logger) withRequestID(id RequestID) Logger {
	if id == "" {
		logger.Warn("Request ID is empty")
		return logger
	}
	if logger.id != "" {
		logger.Infow("Request ID already set in logger. Using existing Request ID", "current request-id", logger.id, "new request-id", id)
		return logger
	}
	valuesWithRequestID := logger.appendValueMap(map[string]interface{}{"request-id": id})
	return Logger{
		SugaredLogger: logger.SugaredLogger.With("request-id", id),
		id:     id,
		values: valuesWithRequestID,
	}
}

func (logger Logger) With(args ...interface{}) Logger {
	if len(args)%2 != 0 {
	    GlobalLogger.Errorw("Odd number of arguments passed to With. Skipping.", "args", args)
	    return logger
	}
	valueMap := make(map[string]interface{})
	for i := 0; i < len(args); i += 2 {
		str, ok := args[i].(string)
		if !ok {
			GlobalLogger.Errorw(
				"Unable to add args in logger with",
				"args", args, "not string", args[i],
			)
			return logger
		}
		valueMap[str] = args[i + 1]
	}
	return Logger{
		SugaredLogger: logger.SugaredLogger.With(args...),
		id:     logger.id,
		values: logger.appendValueMap(valueMap),
	}
}

func (logger Logger) LogIfErr(msg string, err error) {
	if err != nil {
		logger.Errorw("Deferred error failed.", "msg", msg, "err", err)
	}
}

func (logger Logger) WithResource(resourceType ResourceType, name, variant string) Logger {
	newValues := make(map[string]interface{})
	if resourceType != "" {
		newValues["resource-type"] = resourceType
		logger.SugaredLogger = logger.SugaredLogger.With("resource-type", resourceType)
	} else {
		logger.Warn("Resource type is an empty string")
	}

	if name != "" {
		newValues["resource-name"] = name
		logger.SugaredLogger = logger.SugaredLogger.With("resource-name", name)
	} else {
		logger.Warn("Resource name is empty")
	}

	if variant != "" {
		newValues["resource-variant"] = variant
		logger.SugaredLogger = logger.SugaredLogger.With("resource-variant", variant)
	}

	combinedValues := logger.appendValueMap(newValues)

	return Logger{
		SugaredLogger: logger.SugaredLogger,
		id:            logger.id,
		values:        combinedValues,
	}
}

func (logger Logger) WithProvider(providerType, providerName string) Logger {
	newValues := make(map[string]interface{})
	if providerType != "" {
		newValues["provider-type"] = providerType
		logger.SugaredLogger = logger.SugaredLogger.With("provider-type", providerType)
	} else {
		logger.Warn("Provider type is empty")
	}

	if providerName != "" {
		newValues["provider-name"] = providerName
		logger.SugaredLogger = logger.SugaredLogger.With("provider-name", providerName)
	} else {
		logger.Warn("Provider name is empty")
	}

	combinedValues := logger.appendValueMap(newValues)

	return Logger{
		SugaredLogger: logger.SugaredLogger,
		id:            logger.id,
		values:        combinedValues,
	}
}

func (logger Logger) WithValues(values map[string]interface{}) Logger {
	if values == nil {
		logger.Warn("Values are empty")
	}
	combinedValues := logger.appendValueMap(values)
	return Logger{
		SugaredLogger: logger.SugaredLogger,
		id:            logger.id,
		values:        combinedValues,
	}
}

func (logger Logger) GetValue(key string) interface{} {
	value, ok := logger.values.Load(key)
	if !ok {
		logger.Infow("Value not found", "key", key)
	}
	return value
}

func (logger Logger) appendValueMap(values map[string]interface{}) *sync.Map {
	combinedValues := &sync.Map{}
	logger.values.Range(func(key, value interface{}) bool {
		combinedValues.Store(key, value)
		return true
	})
	for k, v := range values {
		combinedValues.Store(k, v)
	}
	return combinedValues
}

func (logger Logger) InitializeRequestID(ctx context.Context) (string, context.Context, Logger) {
	requestID := ctx.Value(RequestIDKey)
	if requestID == nil {
		logger.Debugw("Creating new Request ID", "request-id", requestID)
		requestID = NewRequestID()
		ctx = context.WithValue(ctx, RequestIDKey, requestID)
	}
	ctxLogger := ctx.Value(LoggerKey)
	if ctxLogger == nil {
		logger.Debugw("Adding logger to context")
		ctxLogger = logger.withRequestID(requestID.(RequestID))
		ctx = context.WithValue(ctx, LoggerKey, ctxLogger)
	}
	return requestID.(RequestID).String(), ctx, ctxLogger.(Logger)
}

func GetRequestIDFromContext(ctx context.Context) string {
	requestID := ctx.Value(RequestIDKey)
	if requestID == nil {
		GlobalLogger.Warn("Request ID not found in context")
		return ""
	}

	return requestID.(RequestID).String()
}

func GetLoggerFromContext(ctx context.Context) Logger {
	logger := ctx.Value(LoggerKey)
	if logger == nil {
		GlobalLogger.Warn("Logger not found in context")
		return NewLoggerWithLevel("logger", InfoLevel)
	}
	return logger.(Logger)
}

func (logger Logger) GetRequestID() RequestID {
	return logger.id
}

func AttachRequestID(id string, ctx context.Context, logger Logger) context.Context {
	if ctx == nil {
		logger.Warn("Context is nil")
		return nil
	}
	contextID := ctx.Value(RequestIDKey)
	if contextID != nil {
		logger.Infow("Request ID already set in context. Overwriting request ID", "old request-id", contextID, "new request-id", id)
	}
	ctx = context.WithValue(ctx, RequestIDKey, RequestID(id))
	logger = logger.withRequestID(RequestID(id))
	ctx = context.WithValue(ctx, LoggerKey, logger)
	return ctx
}

func AddLoggerToContext(ctx context.Context, logger Logger) context.Context {
	contextLogger := ctx.Value(LoggerKey)
	if contextLogger == nil {
		ctx = context.WithValue(ctx, LoggerKey, logger)
	}
	return ctx
}

func NewTestLogger(t *testing.T) Logger {
	return WrapZapLogger(zaptest.NewLogger(t).Sugar())
}

func NewLogger(service string) Logger {
	if config.ShouldUseDebugLogging() {
		return NewLoggerWithLevel(service, DebugLevel)
	} else {
		return NewLoggerWithLevel(service, InfoLevel)
	}
}

func NewLoggerWithLevel(service string, loggerLevel string) Logger {
	var baseLogger *zap.Logger
	var err error
	if loggerLevel == InfoLevel {
		baseLogger, err = zap.NewProduction(zap.AddStacktrace(zap.WarnLevel))
		if err != nil {
			panic(err)
		}
	} else if loggerLevel == DebugLevel {
		baseLogger, err = zap.NewDevelopment(zap.AddStacktrace(zap.WarnLevel))
		if err != nil {
			panic(err)
		}
	} else {
		panic("Invalid value for FEATUREFORM_DEBUG_LOGGING")
	}
	logger := baseLogger.Sugar().Named(service)
	logger.Infof("New logger created with log level %s", loggerLevel)
	return Logger{
		SugaredLogger: logger,
		values:        &sync.Map{},
	}
}

func WrapZapLogger(sugaredLogger *zap.SugaredLogger) Logger {
	return Logger{
		SugaredLogger: sugaredLogger,
		values:        &sync.Map{},
	}
}

func NewStackTraceLogger(service string) Logger {
	cfg := zap.Config{
		Encoding:         "json",
		Level:            zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:      true,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			NewReflectedEncoder: func(w io.Writer) zapcore.ReflectedEncoder {
				enc := json.NewEncoder(w)
				enc.SetEscapeHTML(false)
				enc.SetIndent("", "    ")
				return enc
			},
		},
	}
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	return Logger{
		SugaredLogger: logger.Sugar().Named(service),
		id:            "",
	}
}
