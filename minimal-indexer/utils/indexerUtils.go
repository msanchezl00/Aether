package utils

import (
	"encoding/json"
	models "minimal-indexer/Models"
	"minimal-indexer/modules/config"
)

// Define una plantilla para un Array de Strings (StringArraySchema)
var StringArraySchema models.FieldSchema = models.FieldSchema{
	Type: "array",
	Items: &models.FieldSchema{
		Type:     "string",
		Optional: false,
	},
	Optional: false,
}

// BuildPayload construye el mensaje completo (Schema + Payload) listo para Kafka.
func BuildPayload(payload models.KafkaIndexerPayload) []byte {

	// --- 1. Definiciones de Esquemas Anidados ---

	// Esquema de Links
	linksSchema := models.FieldSchema{
		Type:     "struct",
		Optional: false,
		Field:    "links",
		Name:     "Links",
		Fields: []models.FieldSchema{
			{Type: "array", Optional: false, Field: "http", Items: StringArraySchema.Items},
			{Type: "array", Optional: false, Field: "https", Items: StringArraySchema.Items},
			{Type: "array", Optional: false, Field: "files", Items: StringArraySchema.Items},
			{Type: "array", Optional: false, Field: "internal", Items: StringArraySchema.Items},
		},
	}

	// Esquema de Imgs
	imgsSchema := models.FieldSchema{
		Type:     "struct",
		Optional: false,
		Field:    "imgs",
		Name:     "Imgs",
		Fields: []models.FieldSchema{
			{Type: "array", Optional: false, Field: "imgs", Items: StringArraySchema.Items},
		},
	}

	// Esquema de Texts
	textsSchema := models.FieldSchema{
		Type:     "struct",
		Optional: false,
		Field:    "texts",
		Name:     "Texts",
		Fields: []models.FieldSchema{
			{Type: "array", Optional: false, Field: "h1", Items: StringArraySchema.Items},
			{Type: "array", Optional: false, Field: "h2", Items: StringArraySchema.Items},
			{Type: "array", Optional: false, Field: "p", Items: StringArraySchema.Items},
		},
	}

	// --- 🚨 CORRECCIÓN CRÍTICA PARA EL TIPO MAP Metadata.Other ---

	// 1. Definición del Esquema de la CLAVE del mapa (String)
	mapKeySchema := models.FieldSchema{
		Type:     "string",
		Optional: false,
	}

	// 2. Definición del Esquema del VALOR del mapa (Array de Strings)
	mapValueSchema := models.FieldSchema{
		Type:     "array",
		Optional: false,
		Items: &models.FieldSchema{ // El contenido del array es un string
			Type:     "string",
			Optional: false,
		},
	}

	// Esquema de Metadata (incluye el tipo MAP CORREGIDO para 'Other')
	metadataSchema := models.FieldSchema{
		Type:     "struct",
		Optional: false,
		Field:    "metadata",
		Name:     "Metadata",
		Fields: []models.FieldSchema{
			{Type: "array", Optional: false, Field: "charset", Items: StringArraySchema.Items},
			{Type: "array", Optional: false, Field: "logo", Items: StringArraySchema.Items},
			{Type: "array", Optional: false, Field: "scripts", Items: StringArraySchema.Items},
			{Type: "array", Optional: false, Field: "styles", Items: StringArraySchema.Items},
			{Type: "array", Optional: false, Field: "title", Items: StringArraySchema.Items},

			// Campo 'Other': MAPA Corregido
			{
				Type:     "map",
				Optional: false,
				Field:    "other",

				// 🟢 Usa 'Key' para la clave
				Key: &mapKeySchema,

				// 🟢 Usa 'Value' para el valor
				Value: &mapValueSchema,
			},
		},
	}

	// --- 2. Esquema de ContentPayload (Nivel Superior de Contenido) ---
	contentPayloadSchema := models.FieldSchema{
		Type:     "struct",
		Optional: false,
		Field:    "content",
		Name:     "ContentPayload",
		Fields: []models.FieldSchema{
			linksSchema,
			metadataSchema,
			imgsSchema,
			textsSchema,
		},
	}

	// --- 3. Esquema Principal (Nivel de KafkaIndexerPayload) ---
	mainSchema := models.ConnectSchema{
		Type:     "struct",
		Optional: false,
		Name:     "KafkaIndexerPayload",
		Fields: []models.FieldSchema{
			// Campos de Particionamiento
			{Type: "string", Optional: false, Field: "domain"},
			{Type: "string", Optional: false, Field: "path"},
			{Type: "string", Optional: false, Field: "date"},

			// Tags (Array de Strings)
			{Type: "array", Optional: false, Field: "tags", Items: StringArraySchema.Items},

			// Contenido Anidado
			contentPayloadSchema,
		},
	}

	// 4. Estructura final que va a Kafka (ConnectMessage)
	connectMessage := models.ConnectMessage{
		Schema:  mainSchema,
		Payload: payload,
	}

	// 5. Serializa a []byte
	kafkaValue, err := json.Marshal(connectMessage)
	if err != nil {
		config.Logger.Errorf("Error al serializar el mensaje de Kafka Connect: %v", err)
		return nil
	}
	return kafkaValue
}
