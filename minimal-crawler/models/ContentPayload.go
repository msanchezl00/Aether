package models

type ContentPayload struct {
	Links    Links    `json:"links"`
	Metadata Metadata `json:"metadata"`
	Imgs     Imgs     `json:"imgs"`
	Texts    Texts    `json:"texts"`
}

type Links struct {
	Http     []string `json:"http"`
	Https    []string `json:"https"`
	Files    []string `json:"files"`
	Internal []string `json:"internal"`
}

type Metadata struct {
	Charset []string            `json:"charset"`
	Logo    []string            `json:"logo"`
	Scripts []string            `json:"scripts"`
	Styles  []string            `json:"styles"`
	Title   []string            `json:"title"`
	Other   map[string][]string `json:"other"`
}

type Imgs struct {
	Imgs []string `json:"imgs"`
}

type Texts struct {
	H1 []string `json:"h1"`
	H2 []string `json:"h2"`
	P  []string `json:"p"`
}
