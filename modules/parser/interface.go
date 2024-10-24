package parser

import "github.com/PuerkitoBio/goquery"

type ParserInterface interface {
	Parse(htmlUTF8 []byte) ([]map[string][]string, error)
	ParseLinks(doc *goquery.Document) (map[string][]string, error)
	ParseMetadata(doc *goquery.Document) (map[string][]string, error)
}
