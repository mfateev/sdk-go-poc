package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

// Host-only test codec. Random nonces and crypto I/O would be rejected if
// either encoding or decoding accidentally executed inside the isolate.
type encryptedCodec struct{ aad string }

func (c *encryptedCodec) WithSerializationContext(ctx converter.SerializationContext) converter.PayloadCodec {
	return &encryptedCodec{aad: fmt.Sprintf("%T:%+v", ctx, ctx)}
}
func (*encryptedCodec) cipher() cipher.AEAD {
	block, err := aes.NewCipher([]byte("01234567890123456789012345678901"))
	check(err)
	aead, err := cipher.NewGCM(block)
	check(err)
	return aead
}
func (c *encryptedCodec) Encode(input []*commonpb.Payload) ([]*commonpb.Payload, error) {
	aead := c.cipher()
	result := make([]*commonpb.Payload, len(input))
	for i, p := range input {
		raw, err := proto.Marshal(p)
		if err != nil {
			return nil, err
		}
		nonce := make([]byte, aead.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return nil, err
		}
		result[i] = &commonpb.Payload{Metadata: map[string][]byte{converter.MetadataEncoding: []byte("binary/encrypted-poc"), "aad": []byte(c.aad)}, Data: aead.Seal(nonce, nonce, raw, []byte(c.aad))}
	}
	return result, nil
}
func (c *encryptedCodec) Decode(input []*commonpb.Payload) ([]*commonpb.Payload, error) {
	aead := c.cipher()
	result := make([]*commonpb.Payload, len(input))
	for i, p := range input {
		if string(p.Metadata[converter.MetadataEncoding]) != "binary/encrypted-poc" {
			result[i] = p
			continue
		}
		// Read authenticated context from the envelope. Standard SDK replay uses
		// synthetic namespace/IDs, and failure conversion may supply no context.
		// A codec must remain able to decode those existing recorded payloads.
		storedAAD := string(p.Metadata["aad"])
		if len(p.Data) < aead.NonceSize() {
			return nil, fmt.Errorf("missing nonce")
		}
		raw, err := aead.Open(nil, p.Data[:aead.NonceSize()], p.Data[aead.NonceSize():], []byte(storedAAD))
		if err != nil {
			return nil, err
		}
		result[i] = new(commonpb.Payload)
		if err := proto.Unmarshal(raw, result[i]); err != nil {
			return nil, err
		}
	}
	return result, nil
}
