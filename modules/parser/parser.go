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

func (s *Service) Parse(htmlUTF8 []byte) ([]map[string][]string, error) {

	// TODO hay que declarar dinamicamente el numero de maps que tendria
	data := make([]map[string][]string, 5)

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

	// parsear links
	metadatas, err := s.ParseMetadata(doc)
	if err != nil {
		return nil, err
	}

	// se junta a la matriz bidimensional el vector de string con los links
	data = append(data, metadatas)

	// imprimir logs para loggear que salga todo bien
	for _, dataMap := range data {
		for title, vector := range dataMap {
			config.Logger.Infof("|:%s:|", title)
			for j, value := range vector {
				config.Logger.Infof("%d: %s", j, value)
			}
		}
	}

	return data, nil
}

func (s *Service) ParseLinks(doc *goquery.Document) (map[string][]string, error) {

	// map clave valor para con clave=links y como valor un vector de strings de la informacion de metadatos
	links := make(map[string][]string)

	// se grepea por las etiquetas <a> y dentro de ellas por la etiqueta css href
	// filtra los hrefs que tengan https y se obtiene el valor del atributo href con attr
	doc.Find("a").Each(func(i int, a *goquery.Selection) {
		link, _ := a.Attr("href")
		if strings.HasPrefix(link, "https") {
			links["links"] = append(links["links"], "link:"+link)
		}
	})

	return links, nil
}

func (s *Service) ParseMetadata(doc *goquery.Document) (map[string][]string, error) {

	// map clave valor para con clave=metadata y como valor un vector de strings de la informacion de metadatos
	metadatas := make(map[string][]string)

	// Extraer metadatos, se chequea primero si hay charset especificado
	// si no lo hay se lee el metadato donde siempre son de esta forma
	// <meta name="description" content="Descripción de la página.">
	doc.Find("meta").Each(func(i int, meta *goquery.Selection) {
		charset, exists := meta.Attr("charset")
		if exists {
			metadatas["metadata"] = append(metadatas["metadata"], "charset:"+charset)
		} else {
			name, exists := meta.Attr("name")
			content, _ := meta.Attr("content")
			if exists {
				// Agregar el par clave-valor al slice
				metadatas["metadata"] = append(metadatas["metadata"], name+":"+content)
			}
		}
	})

	// se extrae el titulo de la pagina
	// <title>titulo de la pagina</title>
	title := doc.Find("title").Text()
	metadatas["metadata"] = append(metadatas["metadata"], "title:"+title)

	// se extrae el archivo de estilos css
	// <link rel="stylesheet" href="estilos.css">
	doc.Find("link").Each(func(i int, meta *goquery.Selection) {
		href, exists := meta.Attr("href")
		if exists {
			if strings.HasSuffix(href, ".css") {
				metadatas["metadata"] = append(metadatas["metadata"], "style:"+href)
			} else if strings.HasSuffix(href, ".png") || strings.HasSuffix(href, ".ico") {
				metadatas["metadata"] = append(metadatas["metadata"], "logo:"+href)
			}
		}
	})

	// se extrae el .js asociado
	// <script src="script.js"></script>
	doc.Find("script").Each(func(i int, meta *goquery.Selection) {
		script, exists := meta.Attr("src")
		if exists {
			if strings.HasPrefix(script, "https") {
				metadatas["metadata"] = append(metadatas["metadata"], "script:"+script)
			}
		}
	})

	return metadatas, nil
}
