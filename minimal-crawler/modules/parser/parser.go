package parser

import (
	"bytes"
	"minimal-crawler/models"
	config "minimal-crawler/modules/config"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

type Service struct {
	ParserConfig config.ParserConfig
}

func (s *Service) Parse(htmlUTF8 []byte) (models.ContentPayload, error) {

	// se declara el datamap donde se guardara toda la informacion de la url en concreto
	data := models.ContentPayload{}

	// creacion de la variable de tipo document goquery para parsear
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(htmlUTF8))
	if err != nil {
		return models.ContentPayload{}, err
	}

	// parsear links
	data.Links, err = s.ParseLinks(doc)
	if err != nil {
		return models.ContentPayload{}, err
	}

	// parsear metadata
	data.Metadata, err = s.ParseMetadata(doc)
	if err != nil {
		return models.ContentPayload{}, err
	}

	// parsear imgs
	data.Imgs, err = s.ParseImages(doc)
	if err != nil {
		return models.ContentPayload{}, err
	}

	// parsear texts
	data.Texts, err = s.ParseTexts(doc)
	if err != nil {
		return models.ContentPayload{}, err
	}

	return data, nil
}

func (s *Service) ParseLinks(doc *goquery.Document) (models.Links, error) {

	// inicializar el map interno
	links := models.Links{}

	// se grepea por las etiquetas <a> y dentro de ellas por la etiqueta css href
	// filtra los hrefs que tengan https y se obtiene el valor del atributo href con attr
	doc.Find("a").Each(func(i int, a *goquery.Selection) {
		link, _ := a.Attr("href")
		if strings.HasPrefix(link, "https") {
			links.Https = append(links.Https, link)
		} else if strings.HasPrefix(link, "http") {
			links.Http = append(links.Http, link)
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
			links.Files = append(links.Files, link)
		} else {
			links.Internal = append(links.Internal, link)
		}
	})

	return links, nil
}

func (s *Service) ParseMetadata(doc *goquery.Document) (models.Metadata, error) {

	// inicializar el map interno
	metadatas := models.Metadata{}
	metadatas.Other = make(map[string][]string)

	// Extraer metadatos, se chequea primero si hay charset especificado
	// si no lo hay se lee el metadato donde siempre son de esta forma
	// <meta name="description" content="Descripción de la página.">
	doc.Find("meta").Each(func(i int, meta *goquery.Selection) {
		charset, exists := meta.Attr("charset")
		if exists {
			metadatas.Charset = append(metadatas.Charset, charset)
		} else {
			name, exists := meta.Attr("name")
			content, _ := meta.Attr("content")
			if exists {
				metadatas.Other[name] = append(metadatas.Other[name], content)
			}
		}
	})

	// se extrae el titulo de la pagina
	// <title>titulo de la pagina</title>
	title := doc.Find("title").Text()
	metadatas.Title = append(metadatas.Title, title)

	// se extrae el archivo de estilos css
	// <link rel="stylesheet" href="estilos.css">
	doc.Find("link").Each(func(i int, link *goquery.Selection) {
		href, exists := link.Attr("href")
		if exists {
			if strings.HasSuffix(href, ".css") {
				metadatas.Styles = append(metadatas.Styles, href)
			} else if strings.HasSuffix(href, ".png") || strings.HasSuffix(href, ".ico") {
				metadatas.Logo = append(metadatas.Logo, href)
			}
		}
	})

	// se extrae el .js asociado
	// <script src="script.js"></script>
	doc.Find("script").Each(func(i int, script *goquery.Selection) {
		link, exists := script.Attr("src")
		if exists {
			if strings.HasPrefix(link, "https") {
				metadatas.Scripts = append(metadatas.Scripts, link)
			}
		}
	})

	return metadatas, nil
}

func (s *Service) ParseImages(doc *goquery.Document) (models.Imgs, error) {

	// inicializar el map interno
	imgs := models.Imgs{}

	doc.Find("img").Each(func(i int, img *goquery.Selection) {
		link, _ := img.Attr("src")
		if strings.HasPrefix(link, "https") {
			imgs.Imgs = append(imgs.Imgs, link)
		}
	})

	return imgs, nil
}

func (s *Service) ParseTexts(doc *goquery.Document) (models.Texts, error) {

	// inicializar el map interno
	texts := models.Texts{}

	doc.Find("h1").Each(func(i int, h1 *goquery.Selection) {
		text := h1.Text()
		texts.H1 = append(texts.H1, text)
	})

	doc.Find("h2").Each(func(i int, h2 *goquery.Selection) {
		text := h2.Text()
		texts.H2 = append(texts.H2, text)
	})

	doc.Find("p").Each(func(i int, p *goquery.Selection) {
		text := p.Text()
		texts.P = append(texts.P, text)
	})

	return texts, nil
}
