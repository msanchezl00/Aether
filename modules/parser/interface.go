package parser

import "github.com/PuerkitoBio/goquery"

type ParserInterface interface {
	Parse(htmlUTF8 []byte) ([][]string, error)
	ParseLinks(doc *goquery.Document) ([]string, error)
}
