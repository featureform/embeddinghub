// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/featureform/fferr"
)

type ValueType interface {
	Scalar() ScalarType
	IsVector() bool
	Type() reflect.Type
}

type VectorType struct {
	ScalarType  ScalarType
	Dimension   int32
	IsEmbedding bool
}

func (t VectorType) Scalar() ScalarType {
	return t.ScalarType
}

func (t VectorType) IsVector() bool {
	return true
}

func (t VectorType) Type() reflect.Type {
	scalar := t.Scalar().Type()
	if scalar.Kind() == reflect.Ptr {
		scalar = scalar.Elem()
	}
	return reflect.SliceOf(scalar)
}

type ScalarType string

func (t ScalarType) Scalar() ScalarType {
	return t
}

func (t ScalarType) IsVector() bool {
	return false
}

// This method is used in encoding our supported data types to parquet.
// It returns a pointer type for scalar values to allow for nullability.
func (t ScalarType) Type() reflect.Type {
	switch t {
	case Int:
		return reflect.PointerTo(reflect.TypeOf(int(0)))
	case Int8:
		return reflect.PointerTo(reflect.TypeOf(int8(0)))
	case Int16:
		return reflect.PointerTo(reflect.TypeOf(int16(0)))
	case Int32:
		return reflect.PointerTo(reflect.TypeOf(int32(0)))
	case Int64:
		return reflect.PointerTo(reflect.TypeOf(int64(0)))
	case UInt8:
		return reflect.PointerTo(reflect.TypeOf(uint8(0)))
	case UInt16:
		return reflect.PointerTo(reflect.TypeOf(uint16(0)))
	case UInt32:
		return reflect.PointerTo(reflect.TypeOf(uint32(0)))
	case UInt64:
		return reflect.PointerTo(reflect.TypeOf(uint64(0)))
	case Float32:
		return reflect.PointerTo(reflect.TypeOf(float32(0)))
	case Float64:
		return reflect.PointerTo(reflect.TypeOf(float64(0)))
	case String:
		return reflect.PointerTo(reflect.TypeOf(string("")))
	case Bool:
		return reflect.PointerTo(reflect.TypeOf(bool(false)))
	case Timestamp:
		return reflect.TypeOf(time.Time{})
	case Datetime:
		return reflect.TypeOf(time.Time{})
	default:
		return nil
	}
}

const (
	NilType   ScalarType = ""
	Int       ScalarType = "int"
	Int8      ScalarType = "int8"
	Int16     ScalarType = "int16"
	Int32     ScalarType = "int32"
	Int64     ScalarType = "int64"
	UInt8     ScalarType = "uint8"
	UInt16    ScalarType = "uint16"
	UInt32    ScalarType = "uint32"
	UInt64    ScalarType = "uint64"
	Float32   ScalarType = "float32"
	Float64   ScalarType = "float64"
	String    ScalarType = "string"
	Bool      ScalarType = "bool"
	Timestamp ScalarType = "time.Time"
	Datetime  ScalarType = "datetime"
)

var ScalarTypes = map[ScalarType]bool{
	NilType:   true,
	Int:       true,
	Int8:      true,
	Int16:     true,
	Int32:     true,
	Int64:     true,
	UInt8:     true,
	UInt16:    true,
	UInt32:    true,
	UInt64:    true,
	Float32:   true,
	Float64:   true,
	String:    true,
	Bool:      true,
	Timestamp: true,
	Datetime:  true,
}

type ValueTypeJSONWrapper struct {
	ValueType
}

func (vt *ValueTypeJSONWrapper) UnmarshalJSON(data []byte) error {
	v := map[string]VectorType{"ValueType": {}}
	if err := json.Unmarshal(data, &v); err == nil {
		vt.ValueType = v["ValueType"]
		return nil
	}

	s := map[string]ScalarType{"ValueType": ScalarType("")}
	if err := json.Unmarshal(data, &s); err == nil {
		vt.ValueType = s["ValueType"]
		return nil
	}

	return fferr.NewInternalError(fmt.Errorf("could not unmarshal value type: %v", data))
}

func (vt ValueTypeJSONWrapper) MarshalJSON() ([]byte, error) {
	switch vt.ValueType.(type) {
	case VectorType:
		return json.Marshal(map[string]VectorType{"ValueType": vt.ValueType.(VectorType)})
	case ScalarType:
		return json.Marshal(map[string]ScalarType{"ValueType": vt.ValueType.(ScalarType)})
	default:
		return nil, fferr.NewInternalError(fmt.Errorf("could not marshal value type: %v", vt.ValueType))
	}
}
