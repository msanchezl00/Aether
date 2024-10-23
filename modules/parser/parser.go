package parser

import (
	"bytes"
	config "minimal-crawler/modules/config"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

type Service struct {
	ParserConfig config.ParserConfig
}

func (s *Service) Parse(htmlUTF8 []byte) ([][]string, error) {

	var data [][]string

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(htmlUTF8))
	if err != nil {
		return nil, err
	}

	links, err := s.ParseLinks(doc)
	if err != nil {
		return nil, err
	}

	data = append(data, links)

	return data, nil
}

func (s *Service) ParseLinks(doc *goquery.Document) ([]string, error) {
	links := doc.Find("a").FilterFunction(func(i int, a *goquery.Selection) bool {
		link, _ := a.Attr("href")
		return strings.HasPrefix(link, "https")
	}).Map(func(i int, a *goquery.Selection) string {
		link, _ := a.Attr("href")
		return link
	})

	for i, link := range links {
		config.Logger.Infof("%d: %s", i, link)
	}
	return links, nil
}
