package transformer

import (
	models "minimal-indexer/Models"
	"minimal-indexer/modules/config"
	"minimal-indexer/utils"
	"regexp"
	"strings"
)

type Service struct {
	TransformerConfig config.TransformerConfig
}

func (s *Service) Transform(payload models.KafkaCrawlerPayload) (models.KafkaIndexerPayload, error) {

	domain, _ := utils.ExtractDomain(payload.URL)
	path, _ := utils.ExtractPath(payload.URL)

	tags := ExtractTags(domain, path, payload.Content, s.TransformerConfig.Keywords, s.TransformerConfig.Patterns)

	indexPayload := models.KafkaIndexerPayload{
		Domain:  domain,
		Path:    path,
		Tags:    tags,
		Content: payload.Content,
	}

	config.Logger.Infof("transformed payload for url %s with %d tags", payload.URL, len(tags))

	return indexPayload, nil
}

// Extrae entidades dinámicas de frases contextuales
func extractDynamicKeywords(text string, patterns []string) []string {
	text = strings.ToLower(text)
	var results []string

	for _, pat := range patterns {
		re := regexp.MustCompile(pat)
		matches := re.FindAllStringSubmatch(text, -1)
		for _, m := range matches {
			if len(m) > 1 {
				segment := strings.TrimSpace(m[1])

				// dividir por conectores (and, y, ,)
				parts := regexp.MustCompile(`\s*(,|y|and)\s*`).Split(segment, -1)
				for _, p := range parts {
					p = strings.TrimSpace(p)
					if len(p) > 2 && len(strings.Fields(p)) <= 4 {
						results = append(results, p)
					}
				}
			}
		}
	}
	return results
}

// Etiquetado completo
func ExtractTags(domain string, path string, c models.ContentPayload, keywordLibrary map[string][]string, patterns []string) []string {
	tags := make(map[string]bool)

	allTexts := append(c.Texts.H1, c.Texts.H2...)
	allTexts = append(allTexts, c.Texts.P...)
	text := strings.ToLower(strings.Join(allTexts, " "))

	// Etiquetas fijas
	for tag, words := range keywordLibrary {
		for _, w := range words {
			if strings.Contains(text, w) {
				tags[tag] = true
				break
			}
		}
	}

	// Dinámico
	for _, segment := range allTexts {
		for _, found := range extractDynamicKeywords(segment, patterns) {
			tags[strings.TrimSpace(found)] = true
		}
	}

	// Etiquetas técnicas
	if len(c.Metadata.Logo) > 0 {
		tags["marca"] = true
	}
	if len(c.Metadata.Scripts) > 0 {
		tags["javascript"] = true
	}
	if len(c.Metadata.Styles) > 0 {
		tags["css"] = true
	}
	if len(c.Links.Https) > 0 {
		tags["sitio_seguro"] = true
	}
	if len(c.Links.Files) > 0 {
		tags["descargas"] = true
	}

	// Etiquetas basadas en dominio y path
	d := strings.ToLower(domain)
	if strings.Contains(d, "github") {
		tags["repositorio"] = true
	}
	if strings.Contains(d, "shop") || strings.Contains(d, "store") {
		tags["ecommerce"] = true
	}
	if strings.Contains(d, "blog") {
		tags["blog"] = true
	}

	p := strings.ToLower(path)
	if strings.Contains(p, "contact") {
		tags["contacto"] = true
	}
	if strings.Contains(p, "login") || strings.Contains(p, "signin") {
		tags["autenticación"] = true
	}
	if strings.Contains(p, "product") || strings.Contains(p, "service") {
		tags["producto"] = true
	}
	if strings.Contains(p, "news") || strings.Contains(p, "blog") {
		tags["noticias"] = true
	}

	var result []string
	for k := range tags {
		result = append(result, k)
	}
	return result
}
