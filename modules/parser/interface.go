package parser

import "github.com/PuerkitoBio/goquery"

type ParserInterface interface {
	Parse(htmlUTF8 []byte) ([]byte, error)
	ParseLinks(doc *goquery.Document) (map[string]map[string][]string, error)
	ParseMetadata(doc *goquery.Document) (map[string]map[string][]string, error)
	ParseImages(doc *goquery.Document) (map[string]map[string][]string, error)
	ParseTexts(doc *goquery.Document) (map[string]map[string][]string, error)
}
