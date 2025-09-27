package utils

import (
	"encoding/json"
	"minimal-crawler/models"
	"net/url"
	"strings"
	"time"
)

func VerifyDomainsAndInternal(crawledDomains []string, newRawUrls []string, crawledInternalURLs []string, newInternalURLs []string, mainRawURL string) ([]string, []string, error) {
	// Crear un mapa para los dominios crawled
	crawledDomainsMap := make(map[string]struct{})
	for _, domain := range crawledDomains {
		crawledDomainsMap[domain] = struct{}{}
	}

	// Crear un mapa para las urls internas crawled
	crawledInternalURLsMap := make(map[string]struct{})
	for _, internalURL := range crawledInternalURLs {
		crawledInternalURLsMap[internalURL] = struct{}{}
	}

	// Slice para almacenar los dominios no verificados
	var notCrawledDomains []string
	for _, rawURL := range newRawUrls {
		domain, err := ExtractDomain(rawURL)
		if err != nil {
			return nil, nil, err // Manejo de errores si ExtractDomain falla
		}
		if _, exists := crawledDomainsMap[domain]; !exists {
			scheme := "http://" // Valor por defecto
			if strings.HasPrefix(rawURL, "https://") {
				scheme = "https://"
			} else if strings.HasPrefix(rawURL, "http://") {
				scheme = "http://"
			}
			notCrawledDomains = append(notCrawledDomains, scheme+domain)
		}
	}

	// Slice para almacenar los urls internas no crawleadas
	var notCrawledInternalURLs []string
	for _, internalURL := range newInternalURLs {
		if _, exists := crawledInternalURLsMap[internalURL]; !exists {
			notCrawledInternalURLs = append(notCrawledInternalURLs, internalURL)
		}
	}

	return notCrawledDomains, notCrawledInternalURLs, nil
}

func ExtractDomain(rawURL string) (string, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	return parsedURL.Hostname(), nil
}

func ExtractScheme(rawURL string) (string, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	return parsedURL.Scheme, nil
}

func ExtractExternalURLs(parsedData []map[string]map[string][]string) []string {
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

func ExtractInternalURLs(parsedData []map[string]map[string][]string, rawURL string) []string {

	var updatedInternalURLs []string
	var internalURLs []string
	rawURLDomain, err := ExtractDomain(rawURL)
	if err != nil {
		return nil // Manejo de errores si ExtractDomain falla
	}
	rawURLPrefix, err := ExtractScheme(rawURL)
	if err != nil {
		return nil // Manejo de errores si ExtractDomain falla
	}
	for _, block := range parsedData {
		if links, exists := block["links"]; exists {
			// los puntos suspensivos es para expandir el slice ya que se le esta apendeando un vector de strings
			if _, exists := links["internal"]; exists {
				internalURLs = append(internalURLs, links["internal"]...)
			}

			for _, internalURL := range internalURLs {
				if !strings.HasPrefix(internalURL, "//") && !strings.HasPrefix(internalURL, "mailto") && !strings.HasPrefix(internalURL, "tel") && !strings.HasPrefix(internalURL, "#") && internalURL != "" && internalURL != "/" {
					if strings.HasPrefix(internalURL, "/") {
						updatedInternalURLs = append(updatedInternalURLs, rawURLPrefix+"://"+rawURLDomain+internalURL)
					} else {
						updatedInternalURLs = append(updatedInternalURLs, rawURLPrefix+"://"+rawURLDomain+"/"+internalURL)
					}
				}
			}
		}
	}
	return updatedInternalURLs
}

func RemoveDomain(slice []string, domain string) []string {
	for i, notCrawledURL := range slice {
		notCrawledDomain, _ := ExtractDomain(notCrawledURL)
		if notCrawledDomain == domain {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

func BuildPayload(rawURL string, payload []map[string]map[string][]string) []byte {

	wrapped := models.KafkaCrawlerPayload{
		URL:       rawURL,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}

	value, _ := json.Marshal(wrapped)

	return value
}
