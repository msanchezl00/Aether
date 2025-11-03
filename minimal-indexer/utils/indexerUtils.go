package utils

import (
	"bytes"
	"encoding/binary"
	"log"

	models "minimal-indexer/Models"

	"github.com/linkedin/goavro/v2"
	srclient "github.com/riferrei/srclient"
)

func BuildPayloadAvro(payload models.KafkaIndexerPayload) []byte {
	schemaRegistryURL := "http://schema-registry:8081"
	client := srclient.CreateSchemaRegistryClient(schemaRegistryURL)

	// Obtener o registrar schema
	schema, err := client.GetLatestSchema("parquet_data-value")
	if err != nil {
		schema, err = client.CreateSchema("parquet_data-value", models.KafkaIndexerAvroSchema, srclient.Avro)
		if err != nil {
			log.Printf("Error registrando schema: %v", err)
			return nil
		}
	}

	// Crear codec Avro a partir del schema
	codec, err := goavro.NewCodec(models.KafkaIndexerAvroSchema)
	if err != nil {
		log.Printf("Error creando codec Avro: %v", err)
		return nil
	}

	// Convertir struct Go a map[string]interface{}
	native := TransformAvroToNative(payload)

	// Codificar nativo a Avro binario
	binaryAvro, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		log.Printf("Error serializando payload a Avro: %v", err)
		return nil
	}

	// Armar mensaje con magic byte + schema ID + Avro binario
	var msg bytes.Buffer
	msg.WriteByte(0) // Magic byte 0x00
	binary.Write(&msg, binary.BigEndian, int32(schema.ID()))
	msg.Write(binaryAvro)

	return msg.Bytes()
}

func TransformAvroToNative(payload models.KafkaIndexerPayload) map[string]interface{} {
	return map[string]interface{}{
		"domain":    map[string]interface{}{"string": payload.Domain},
		"path":      map[string]interface{}{"string": payload.Path},
		"date":      map[string]interface{}{"string": payload.Date},
		"real_path": map[string]interface{}{"string": payload.RealPath},
		"tags":      payload.Tags,
		"content": map[string]interface{}{
			"links": map[string]interface{}{
				"http":     payload.Content.Links.Http,
				"https":    payload.Content.Links.Https,
				"files":    payload.Content.Links.Files,
				"internal": payload.Content.Links.Internal,
			},
			"metadata": map[string]interface{}{
				"charset": payload.Content.Metadata.Charset,
				"logo":    payload.Content.Metadata.Logo,
				"scripts": payload.Content.Metadata.Scripts,
				"styles":  payload.Content.Metadata.Styles,
				"title":   payload.Content.Metadata.Title,
				"other":   payload.Content.Metadata.Other,
			},
			"imgs": map[string]interface{}{
				"imgs": payload.Content.Imgs.Imgs,
			},
			"texts": map[string]interface{}{
				"h1": payload.Content.Texts.H1,
				"h2": payload.Content.Texts.H2,
				"p":  payload.Content.Texts.P,
			},
		},
	}
}
