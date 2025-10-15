package models

type KafkaIndexerPayload struct {
	Domain  string         `json:"domain"`
	Path    string         `json:"path"`
	Date    string         `json:"date"`
	Tags    []string       `json:"tags"`
	Content ContentPayload `json:"content"`
}

// FieldSchema define un campo simple, un array, o una struct anidada.
type FieldSchema struct {
	Type     string        `json:"type"`
	Optional bool          `json:"optional"`
	Field    string        `json:"field"`
	Name     string        `json:"name,omitempty"`
	Items    *FieldSchema  `json:"items,omitempty"`
	Key      *FieldSchema  `json:"key,omitempty"`
	Value    *FieldSchema  `json:"value,omitempty"`
	Fields   []FieldSchema `json:"fields,omitempty"`
}

// ConnectSchema: El esquema de nivel superior.
type ConnectSchema struct {
	Type     string        `json:"type"`
	Optional bool          `json:"optional"`
	Name     string        `json:"name"`
	Fields   []FieldSchema `json:"fields"`
}

// ConnectMessage: La estructura final que se serializa.
type ConnectMessage struct {
	Schema  ConnectSchema       `json:"schema"`
	Payload KafkaIndexerPayload `json:"payload"`
}
