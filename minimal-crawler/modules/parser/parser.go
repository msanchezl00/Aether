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

func (s *Service) Parse(htmlUTF8 []byte) ([]map[string]map[string][]string, error) {

	// se declara el datamap donde se guardara toda la informacion de la url en concreto
	data := make([]map[string]map[string][]string, 0)

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

	// parsear metadata
	metadatas, err := s.ParseMetadata(doc)
	if err != nil {
		return nil, err
	}
	data = append(data, metadatas)

	// parsear imgs
	imgs, err := s.ParseImages(doc)
	if err != nil {
		return nil, err
	}
	data = append(data, imgs)

	// parsear texts
	texts, err := s.ParseTexts(doc)
	if err != nil {
		return nil, err
	}

	data = append(data, texts)

	return data, nil
}

func (s *Service) ParseLinks(doc *goquery.Document) (map[string]map[string][]string, error) {

	// map clave valor para con clave=links y como valor un vector de strings de la informacion de metadatos
	links := make(map[string]map[string][]string)
	// inicializar el map interno
	links["links"] = make(map[string][]string)

	// se grepea por las etiquetas <a> y dentro de ellas por la etiqueta css href
	// filtra los hrefs que tengan https y se obtiene el valor del atributo href con attr
	doc.Find("a").Each(func(i int, a *goquery.Selection) {
		link, _ := a.Attr("href")
		if strings.HasPrefix(link, "https") {
			links["links"]["https"] = append(links["links"]["https"], link)
		} else if strings.HasPrefix(link, "http") {
			links["links"]["http"] = append(links["links"]["http"], link)
		} else if strings.HasSuffix(link, "pdf") ||
			strings.HasSuffix(link, "xlsx") ||
			strings.HasSuffix(link, "xls") ||
			strings.HasSuffix(link, "docx") ||
			strings.HasSuffix(link, "doc") ||
			strings.HasSuffix(link, "pptx") ||
			strings.HasSuffix(link, "ppt") ||
			strings.HasSuffix(link, "txt") ||
			strings.HasSuffix(link, "csv") ||
			strings.HasSuffix(link, "zip") ||
			strings.HasSuffix(link, "rar") {
			links["links"]["files"] = append(links["links"]["files"], link)
		} else {
			links["links"]["internal"] = append(links["links"]["internal"], link)
		}
	})

	return links, nil
}

func (s *Service) ParseMetadata(doc *goquery.Document) (map[string]map[string][]string, error) {

	// map clave valor para con clave=metadata y como valor un vector de strings de la informacion de metadatos
	metadatas := make(map[string]map[string][]string)
	// inicializar el map interno
	metadatas["metadata"] = make(map[string][]string)

	// Extraer metadatos, se chequea primero si hay charset especificado
	// si no lo hay se lee el metadato donde siempre son de esta forma
	// <meta name="description" content="Descripción de la página.">
	doc.Find("meta").Each(func(i int, meta *goquery.Selection) {
		charset, exists := meta.Attr("charset")
		if exists {
			metadatas["metadata"]["charset"] = append(metadatas["metadata"]["charset"], charset)
		} else {
			name, exists := meta.Attr("name")
			content, _ := meta.Attr("content")
			if exists {
				metadatas["metadata"][name] = append(metadatas["metadata"][name], content)
			}
		}
	})

	// se extrae el titulo de la pagina
	// <title>titulo de la pagina</title>
	title := doc.Find("title").Text()
	metadatas["metadata"]["title"] = append(metadatas["metadata"]["title"], title)

	// se extrae el archivo de estilos css
	// <link rel="stylesheet" href="estilos.css">
	doc.Find("link").Each(func(i int, link *goquery.Selection) {
		href, exists := link.Attr("href")
		if exists {
			if strings.HasSuffix(href, ".css") {
				metadatas["metadata"]["styles"] = append(metadatas["metadata"]["styles"], href)
			} else if strings.HasSuffix(href, ".png") || strings.HasSuffix(href, ".ico") {
				metadatas["metadata"]["logo"] = append(metadatas["metadata"]["logo"], href)
			}
		}
	})

	// se extrae el .js asociado
	// <script src="script.js"></script>
	doc.Find("script").Each(func(i int, script *goquery.Selection) {
		link, exists := script.Attr("src")
		if exists {
			if strings.HasPrefix(link, "https") {
				metadatas["metadata"]["scripts"] = append(metadatas["metadata"]["scripts"], link)
			}
		}
	})

	return metadatas, nil
}

func (s *Service) ParseImages(doc *goquery.Document) (map[string]map[string][]string, error) {

	// map clave valor para con clave=metadata y como valor un vector de strings de la informacion de metadatos
	imgs := make(map[string]map[string][]string)
	// inicializar el map interno
	imgs["imgs"] = make(map[string][]string)

	doc.Find("img").Each(func(i int, img *goquery.Selection) {
		link, _ := img.Attr("src")
		if strings.HasPrefix(link, "https") {
			imgs["imgs"]["imgs"] = append(imgs["imgs"]["imgs"], link)
		}
	})

	return imgs, nil
}

func (s *Service) ParseTexts(doc *goquery.Document) (map[string]map[string][]string, error) {
	// TODO aqui habria que obtener mas tipos de texto, como <div>, <article>, <ul>...
	// map clave valor para con clave=metadata y como valor un vector de strings de la informacion de metadatos
	texts := make(map[string]map[string][]string)
	// inicializar el map interno
	texts["texts"] = make(map[string][]string)

	doc.Find("h1").Each(func(i int, h1 *goquery.Selection) {
		text := h1.Text()
		texts["texts"]["h1"] = append(texts["texts"]["h1"], text)
	})

	doc.Find("h2").Each(func(i int, h2 *goquery.Selection) {
		text := h2.Text()
		texts["texts"]["h2"] = append(texts["texts"]["h2"], text)
	})

	doc.Find("p").Each(func(i int, p *goquery.Selection) {
		text := p.Text()
		texts["texts"]["p"] = append(texts["texts"]["p"], text)
	})

	return texts, nil
}
