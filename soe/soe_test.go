package soe_test

import (
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/soe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: use different struct than ./cmd/avrogen/testdata/golden_encoders.go
// or find a way to reuse.

// Test is a test struct.
type Test struct {
	// SomeString is a string.
	SomeString string `avro:"someString"`
	SomeInt    int    `avro:"someInt"`
}

var schemaTest = avro.MustParse(`{"name":"a.b.test","type":"record","fields":[{"name":"someString","type":"string"},{"name":"someInt","type":"int"}]}`)

// Schema returns the schema for Test.
func (o *Test) Schema() avro.Schema {
	return schemaTest
}

// Unmarshal decodes b into the receiver.
func (o *Test) Unmarshal(b []byte) error {
	return avro.Unmarshal(o.Schema(), b, o)
}

// Marshal encodes the receiver.
func (o *Test) Marshal() ([]byte, error) {
	return avro.Marshal(o.Schema(), o)
}

func newAvroGenCodec(t testing.TB) (*soe.AvroGenCodec[*Test], []byte) {
	t.Helper()

	// Ask a nil value for its associated schema definition.
	var nilVal *Test
	schema := nilVal.Schema()

	schemaid, err := soe.GetSchemaID(schema)
	require.NoError(t, err)

	codec, err := soe.NewAvroGenCodec[*Test]()
	require.NoError(t, err)

	return codec, schemaid
}

func TestRoundtrip(t *testing.T) {
	v0 := Test{
		SomeString: "Hello",
		SomeInt: 42,
	}

	codec, schemaid := newAvroGenCodec(t)

	data, err := codec.Marshal(&v0)
	require.NoError(t, err)

	// Check single object header
	assert.Equal(t, data[:2], soe.Magic)
	assert.Equal(t, data[2:10], schemaid)

	// Unmarshal it back from bytes into a struct, and check that we got the same value.
	var v1 Test
	err = codec.Unmarshal(data, &v1)
	require.NoError(t, err)
	assert.Equal(t, v0, v1)
}

func TestUnmarshalTooShort(t *testing.T) {
	codec, _ := newAvroGenCodec(t)

	// Needs to be at least 10 bytes
	data := append(soe.Magic, 0x00, 0x01)

	// Unmarshal the garbage and check error
	var v Test
	err := codec.Unmarshal(data, &v)
	assert.ErrorContains(t, err, "too short")
}

func TestUnmarshalBadMagic(t *testing.T) {
	codec, schemaid := newAvroGenCodec(t)

	data := []byte{
		// Valid magic is c301, use something else
		0x00, 0x00,
	}
	// ... but use a valid schema ID
	data = append(data, schemaid...)

	// Unmarshal the garbage and check error
	var v Test
	err := codec.Unmarshal(data, &v)
	assert.ErrorContains(t, err, "bad message magic")
}

func TestUnmarshalStrictBadSchema(t *testing.T) {
	codec, _ := newAvroGenCodec(t)

	// Valid magic...
	data := soe.Magic
	// ... but use an unknown schema ID
	data = append(data, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)

	// Unmarshal the garbage and check error
	var v Test
	err := codec.UnmarshalStrict(data, &v)
	assert.ErrorContains(t, err, "bad schema")
}
