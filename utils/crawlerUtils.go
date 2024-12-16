package utils

import (
	"net/url"
	"strings"
)

func VerifyDomains(crawledDomains []string, newRawUrls []string) ([]string, error) {
	// Crear un mapa para los dominios crawled
	crawledMap := make(map[string]struct{})
	for _, domain := range crawledDomains {
		crawledMap[domain] = struct{}{}
	}

	// Slice para almacenar los dominios no verificados
	var notCrawledDomains []string

	for _, rawURL := range newRawUrls {
		domain, err := ExtractDomain(rawURL)
		if err != nil {
			return nil, err // Manejo de errores si ExtractDomain falla
		}
		// Verificar si el dominio ya está en crawledDomains
		// TODO verificar que no se introduzcan dominios repetidos en notcrawleddomains
		// aunque esto ya se verifica al insertar dominio
		// y pensar si poner aqui tambien un mutex aunque ya se mete al insertar dominio
		// puede generar que vaya mas lento este todo.
		if _, exists := crawledMap[domain]; !exists {
			scheme := "http://" // Valor por defecto
			if strings.HasPrefix(rawURL, "https://") {
				scheme = "https://"
			} else if strings.HasPrefix(rawURL, "http://") {
				scheme = "http://"
			}
			notCrawledDomains = append(notCrawledDomains, scheme+domain)
		}
	}

	return notCrawledDomains, nil
}

func ExtractDomain(rawURL string) (string, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	return parsedURL.Hostname(), nil
}

func ExtractURLs(parsedData []map[string]map[string][]string) []string {
	var domains []string
	for _, block := range parsedData {
		if links, exists := block["links"]; exists {
			// los puntos suspensivos es para expandir el slice ya que se le esta apendeando un vector de strings
			if _, exists := links["https"]; exists {
				domains = append(domains, links["https"]...)
			}
			if _, exists := links["http"]; exists {
				domains = append(domains, links["http"]...)
			}
		}
	}
	return domains
}
