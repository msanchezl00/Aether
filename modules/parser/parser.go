package parser

import (
	"bytes"
	"encoding/json"
	config "minimal-crawler/modules/config"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

type Service struct {
	ParserConfig config.ParserConfig
}

func (s *Service) Parse(htmlUTF8 []byte) ([]byte, error) {

	// TODO hay que declarar dinamicamente el numero de maps que tendria
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

	// parsear links
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

	// parsear imgs
	texts, err := s.ParseTexts(doc)
	if err != nil {
		return nil, err
	}
	data = append(data, texts)

	dataByte, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	/* para visualizar el json formateado para hacer pruebas
	dataByte, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return nil, err
	}
	config.Logger.Infof(string(dataByte))
	*/

	return dataByte, nil
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
			links["links"]["links"] = append(links["links"]["links"], link)
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
	doc.Find("link").Each(func(i int, meta *goquery.Selection) {
		href, exists := meta.Attr("href")
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
	doc.Find("script").Each(func(i int, meta *goquery.Selection) {
		script, exists := meta.Attr("src")
		if exists {
			if strings.HasPrefix(script, "https") {
				metadatas["metadata"]["scripts"] = append(metadatas["metadata"]["scripts"], script)
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

	doc.Find("img").Each(func(i int, a *goquery.Selection) {
		link, _ := a.Attr("src")
		if strings.HasPrefix(link, "https") {
			imgs["imgs"]["imgs"] = append(imgs["imgs"]["imgs"], link)
		}
	})

	return imgs, nil
}

func (s *Service) ParseTexts(doc *goquery.Document) (map[string]map[string][]string, error) {
	// map clave valor para con clave=metadata y como valor un vector de strings de la informacion de metadatos
	texts := make(map[string]map[string][]string)
	// inicializar el map interno
	texts["texts"] = make(map[string][]string)

	doc.Find("h1").Each(func(i int, a *goquery.Selection) {
		text := a.Text()
		texts["texts"]["h1"] = append(texts["texts"]["h1"], text)
	})

	doc.Find("h2").Each(func(i int, a *goquery.Selection) {
		text := a.Text()
		texts["texts"]["h2"] = append(texts["texts"]["h2"], text)
	})

	doc.Find("p").Each(func(i int, a *goquery.Selection) {
		text := a.Text()
		texts["texts"]["p"] = append(texts["texts"]["p"], text)
	})

	return texts, nil
}
