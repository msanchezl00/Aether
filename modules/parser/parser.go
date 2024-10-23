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

	// creacion de la variable de tipo document goquery para parsear
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(htmlUTF8))
	if err != nil {
		return nil, err
	}

	// parsear links
	links, err := s.ParseLinks(doc)
	if err != nil {
		return nil, err
	}

	// se junta a la matriz bidimensional el vector de string con los links
	data = append(data, links)

	return data, nil
}

func (s *Service) ParseLinks(doc *goquery.Document) ([]string, error) {

	// se grepea por las etiquetas <a> y dentro de ellas por la etiqueta css href
	// filtra los hrefs que tengan https y se obtiene el valor del atributo href con attr
	links := doc.Find("a").FilterFunction(func(i int, a *goquery.Selection) bool {
		link, _ := a.Attr("href")
		return strings.HasPrefix(link, "https")
	}).Map(func(i int, a *goquery.Selection) string {
		link, _ := a.Attr("href")
		return link
	})

	// imprimir logs para loggear que salga todo bien
	for i, link := range links {
		config.Logger.Infof("%d: %s", i, link)
	}

	return links, nil
}
