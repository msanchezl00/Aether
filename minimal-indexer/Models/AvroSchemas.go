package models

const KafkaIndexerAvroSchema = `{
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
