package utils

import (
	"bytes"
	"encoding/binary"
	"log"

	models "minimal-indexer/Models"

	"github.com/linkedin/goavro/v2"
	srclient "github.com/riferrei/srclient"
)

const kafkaIndexerAvroSchema = `{
  "type": "record",
  "name": "KafkaIndexerPayload",
  "fields": [
    {"name": "domain", "type": ["null","string"], "default": null},
    {"name": "path", "type": ["null","string"], "default": null},
    {"name": "date", "type": ["null","string"], "default": null},
	{"name": "real_path", "type": ["null","string"], "default": null},
    {"name": "tags", "type": {"type":"array","items":"string"}, "default": []},
    {"name": "content", "type": {
      "type": "record",
      "name": "ContentPayload",
      "fields": [
        {"name":"links","type":{
          "type":"record","name":"Links",
          "fields":[
            {"name":"http","type":{"type":"array","items":"string"},"default":[]},
            {"name":"https","type":{"type":"array","items":"string"},"default":[]},
            {"name":"files","type":{"type":"array","items":"string"},"default":[]},
            {"name":"internal","type":{"type":"array","items":"string"},"default":[]}
          ]
        }, "default": {}},
        {"name":"metadata","type":{
          "type":"record","name":"Metadata",
          "fields":[
            {"name":"charset","type":{"type":"array","items":"string"},"default":[]},
            {"name":"logo","type":{"type":"array","items":"string"},"default":[]},
            {"name":"scripts","type":{"type":"array","items":"string"},"default":[]},
            {"name":"styles","type":{"type":"array","items":"string"},"default":[]},
            {"name":"title","type":{"type":"array","items":"string"},"default":[]},
            {"name":"other","type":{"type":"map","values":{"type":"array","items":"string"}},"default":{}}
          ]
        }, "default": {}},
        {"name":"imgs","type":{
          "type":"record","name":"Imgs",
          "fields":[{"name":"imgs","type":{"type":"array","items":"string"},"default":[]}]
        }, "default": {}},
        {"name":"texts","type":{
          "type":"record","name":"Texts",
          "fields":[
            {"name":"h1","type":{"type":"array","items":"string"},"default":[]},
            {"name":"h2","type":{"type":"array","items":"string"},"default":[]},
            {"name":"p","type":{"type":"array","items":"string"},"default":[]}
          ]
        }, "default": {}}
      ]
    }, "default": {}}
  ]
}`

func BuildPayloadAvro(payload models.KafkaIndexerPayload) []byte {
	//TODO: refactorizar este metodo
	schemaRegistryURL := "http://schema-registry:8081"
	client := srclient.CreateSchemaRegistryClient(schemaRegistryURL)

	// Obtener o registrar schema
	schema, err := client.GetLatestSchema("parquet_data-value")
	if err != nil {
		schema, err = client.CreateSchema("parquet_data-value", kafkaIndexerAvroSchema, srclient.Avro)
		if err != nil {
			log.Printf("Error registrando schema: %v", err)
			return nil
		}
	}

	// Crear codec Avro a partir del schema
	codec, err := goavro.NewCodec(kafkaIndexerAvroSchema)
	if err != nil {
		log.Printf("Error creando codec Avro: %v", err)
		return nil
	}

	// Convertir struct Go a map[string]interface{}
	native := map[string]interface{}{
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
