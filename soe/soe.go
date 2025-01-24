package soe

import (
	"bytes"
	"fmt"

	"github.com/hamba/avro/v2"
)

// https://avro.apache.org/docs/1.10.2/spec.html#single_object_encoding
var Magic = []byte{0xc3, 0x01}

func GetSchemaID(schema avro.Schema) ([]byte, error) {
	// Compute a fingerprint
	// TODO: wait for https://github.com/hamba/avro/issues/489
	fingerprint, err := schema.FingerprintUsing(avro.CRC64Avro)
	if err != nil {
		return nil, fmt.Errorf("compute fingerprint: %w", err)
	}
	if len(fingerprint) != 8 {
		return nil, fmt.Errorf("bad fingerprint length: %v", fingerprint)
	}
	return fingerprint, nil
}

// Basic codec for en/decoding hamba/avro-annotated types with a provided schema.
// There's no facilities for ensuring the schema agrees with the types.
type Codec struct {
	api    avro.API
	schema avro.Schema
	header []byte
}

func NewCodec(schema avro.Schema) (*Codec, error) {
	return NewCodecWithConfig(schema, avro.Config{})
}

func NewCodecWithConfig(schema avro.Schema, config avro.Config) (*Codec, error) {
	// Compute a fingerprint
	schemaid, err := GetSchemaID(schema)
	if err != nil {
		return nil, err
	}
	// The SOE header is 2 bytes magic + 8 bytes schema ID.
	// Compose it up-front and memoize it.
	soeHeader := append(Magic, schemaid...)
	return &Codec{
		api:    config.Freeze(),
		schema: schema,
		header: soeHeader,
	}, nil
}

func (c *Codec) Marshal(m any) ([]byte, error) {
	data, err := c.api.Marshal(c.schema, m)
	if err != nil {
		return nil, err
	}

	data = append(c.header, data...)
	return data, nil
}

func (c *Codec) Unmarshal(data []byte, m any) error {
	// Payloads have 2 bytes magic and 8 bytes schema ID
	if len(data) < 10 {
		return fmt.Errorf("message too short: %v", data)
	}

	// Check the single object encoding magic marker
	if !bytes.Equal(data[:2], Magic) {
		return fmt.Errorf("bad message magic: %v", data[:2])
	}

	// Shave off header and unmarshal
	data = data[10:]
	err := c.api.Unmarshal(c.schema, data, m)
	if err != nil {
		return err
	}
	return nil
}

func (c *Codec) UnmarshalStrict(data []byte, m any) error {
	// Payloads have 2 bytes magic and 8 bytes schema ID
	if len(data) < 10 {
		return fmt.Errorf("message too short: %v", data)
	}

	// Check the single object encoding magic marker
	if !bytes.Equal(data[:2], Magic) {
		return fmt.Errorf("bad message magic: %v", data[:2])
	}

	// Check the embedded schema ID
	schemaid := c.header[2:10]
	if !bytes.Equal(data[2:10], schemaid) {
		return fmt.Errorf("bad schema: %v", data[2:10])
	}

	// Shave off header and unmarshal
	data = data[10:]
	err := c.api.Unmarshal(c.schema, data, m)
	if err != nil {
		return err
	}
	return nil
}

// All generated Avro types implement this interface
type AvroGenerated interface {
	Unmarshal(b []byte) error
	Marshal() ([]byte, error)
	Schema() avro.Schema
}

func GetSchema[T AvroGenerated]() avro.Schema {
	// The Schema method returns a global schema, so can be called on a nil receiver.
	var val T
	return val.Schema()
}

// AvroGenCodec is a strongly-typed wrapper around Codec, which uses the schema
// from generated types, and ensures binding between types and schema.
// T must be a pointer type, e.g. AvroGenCodec[*MyValueType].
type AvroGenCodec[T any] struct {
	baseCodec *Codec
}

func NewAvroGenCodec[T AvroGenerated]() (*AvroGenCodec[T], error) {
	return NewAvroGenCodecWithConfig[T](avro.Config{})
}

func NewAvroGenCodecWithConfig[T AvroGenerated](config avro.Config) (*AvroGenCodec[T], error) {
	schema := GetSchema[T]()
	codec, err := NewCodecWithConfig(schema, config)
	if err != nil {
		return nil, err
	}
	return &AvroGenCodec[T]{
		baseCodec: codec,
	}, nil
}

func (c *AvroGenCodec[T]) Marshal(m T) ([]byte, error) {
	return c.baseCodec.Marshal(&m)
}

func (c *AvroGenCodec[T]) Unmarshal(data []byte, m T) error {
	return c.baseCodec.Unmarshal(data, &m)
}

func (c *AvroGenCodec[T]) UnmarshalStrict(data []byte, m T) error {
	return c.baseCodec.UnmarshalStrict(data, &m)
}

type SchemaRegistry interface {
	Get(fingerprint uint64) (avro.Schema, error)
}

// TODO: describe
type DynamicCodec struct {
	api      avro.API
	registry SchemaRegistry
}

func (c *DynamicCodec) Unmarshal(data []byte, m any) error {
	// Payloads have 2 bytes magic and 8 bytes schema ID
	if len(data) < 10 {
		return fmt.Errorf("message too short: %v", data)
	}

	// Check the single object encoding magic marker
	if !bytes.Equal(data[:2], Magic) {
		return fmt.Errorf("bad message magic: %v", data[:2])
	}

	// Check the embedded schema ID
	schemaid := data[2:10]
	fingerprint := (uint64(schemaid[0]) |
		uint64(schemaid[1])<<8 |
		uint64(schemaid[2])<<16 |
		uint64(schemaid[3])<<24 |
		uint64(schemaid[4])<<32 |
		uint64(schemaid[5])<<40 |
		uint64(schemaid[6])<<48 |
		uint64(schemaid[7])<<56)

	// Get a real schema from registry.
	schema, err := c.registry.Get(fingerprint)
	if err != nil {
		return fmt.Errorf("schema lookup for %0x: %w", schemaid, err)
	}

	// Shave off header and unmarshal.
	data = data[10:]
	err = c.api.Unmarshal(schema, data, m)
	if err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
}
