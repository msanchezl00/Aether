package models

type KafkaIndexerPayload struct {
	Domain  string   `json:"domain"`
	Path    string   `json:"path"`
	Date    string   `json:"date"`
	Tags    []string `json:"tags"`
	Content Content  `json:"content"`
}

type Content struct {
	Links    Links    `json:"links"`
	Metadata Metadata `json:"metadata"`
	Imgs     Imgs     `json:"imgs"`
	Texts    Texts    `json:"texts"`
}

type Links struct {
	Http     []string `json:"http"`
	Https    []string `json:"https"`
	Internal []string `json:"internal"`
}

type Metadata struct {
	Generator        []string `json:"generator"`
	HandheldFriendly []string `json:"handheldFriendly"`
	MobileOptimized  []string `json:"mobileOptimized"`
	Charset          []string `json:"charset"`
	Logo             []string `json:"logo"`
	Scripts          []string `json:"scripts"`
	Styles           []string `json:"styles"`
	Title            []string `json:"title"`
	Viewport         []string `json:"viewport"`
}

type Imgs struct {
	Imgs []string `json:"imgs"`
}

type Texts struct {
	H1 []string `json:"h1"`
	H2 []string `json:"h2"`
	P  []string `json:"p"`
}
