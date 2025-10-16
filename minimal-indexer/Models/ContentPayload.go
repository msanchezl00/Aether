package models

type ContentPayload struct {
	Links    Links    `avro:"links"`
	Metadata Metadata `avro:"metadata"`
	Imgs     Imgs     `avro:"imgs"`
	Texts    Texts    `avro:"texts"`
}

type Links struct {
	Http     []string `avro:"http"`
	Https    []string `avro:"https"`
	Files    []string `avro:"files"`
	Internal []string `avro:"internal"`
}

type Metadata struct {
	Charset []string            `avro:"charset"`
	Logo    []string            `avro:"logo"`
	Scripts []string            `avro:"scripts"`
	Styles  []string            `avro:"styles"`
	Title   []string            `avro:"title"`
	Other   map[string][]string `avro:"other"`
}

type Imgs struct {
	Imgs []string `avro:"imgs"`
}

type Texts struct {
	H1 []string `avro:"h1"`
	H2 []string `avro:"h2"`
	P  []string `avro:"p"`
}
