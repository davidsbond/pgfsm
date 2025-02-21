package pgfsm

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

type (
	// The Encoding interface is used to convert Command implementations to and from their byte representation that is
	// stored by the FSM within the database.
	Encoding interface {
		// Encode the value provided, returning its byte representation.
		Encode(any) ([]byte, error)
		// Decode the bytes provided into their literal type.
		Decode([]byte, any) error
	}

	// The JSON type is an Encoding implementation that will encode/decode Command implementations into their json
	// representation using encoding/json.
	JSON struct{}

	// The GOB type is an Encoding implementation that will encode/decode Command implementations into their gob
	// representation using encoding/gob.
	GOB struct{}
)

func (j *JSON) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (j *JSON) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (g *GOB) Encode(v any) ([]byte, error) {
	buffer := new(bytes.Buffer)
	err := gob.NewEncoder(buffer).Encode(v)
	return buffer.Bytes(), err
}

func (g *GOB) Decode(data []byte, v interface{}) error {
	buffer := bytes.NewBuffer(data)
	return gob.NewDecoder(buffer).Decode(v)
}
