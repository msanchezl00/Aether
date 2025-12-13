package utils

import (
	"encoding/json"
	"io"
	"minimal-crawler/models"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func VerifyDomainsAndInternal(crawledDomains map[string]time.Time, newRawUrls []string, crawledInternalURLs []string, newInternalURLs []string, mainRawURL string) ([]string, []string, error) {
	// Crear un mapa para los dominios crawled
	crawledDomainsMap := make(map[string]struct{})
	for domain := range crawledDomains {
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

func ExtractExternalURLs(parsedData models.ContentPayload) []string {
	var domains []string

	domains = append(domains, parsedData.Links.Https...)
	domains = append(domains, parsedData.Links.Http...)

	return domains
}

func ExtractInternalURLs(parsedData models.ContentPayload, rawURL string) []string {

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

	internalURLs = append(internalURLs, parsedData.Links.Internal...)

	for _, internalURL := range internalURLs {
		if !strings.HasPrefix(internalURL, "//") && !strings.HasPrefix(internalURL, "mailto") && !strings.HasPrefix(internalURL, "tel") && !strings.HasPrefix(internalURL, "#") && internalURL != "" && internalURL != "/" {
			if strings.HasPrefix(internalURL, "/") {
				updatedInternalURLs = append(updatedInternalURLs, rawURLPrefix+"://"+rawURLDomain+internalURL)
			} else {
				updatedInternalURLs = append(updatedInternalURLs, rawURLPrefix+"://"+rawURLDomain+"/"+internalURL)
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

func BuildPayload(rawURL string, payload models.ContentPayload) []byte {

	wrapped := models.KafkaCrawlerPayload{
		URL:       rawURL,
		Timestamp: time.Now().UTC(),
		Content:   payload,
	}

	value, _ := json.Marshal(wrapped)

	return value
}

func GetRobotsRules(urlStr string) (allows []string, disallows []string, err error) {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		return nil, nil, err
	}

	baseURL := parsed.Scheme + "://" + parsed.Host

	resp, err := http.Get(baseURL + "/robots.txt")
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	lines := strings.Split(string(data), "\n")
	currentAgent := ""

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.TrimSpace(parts[1])

		switch key {
		case "user-agent":
			currentAgent = val

		case "allow":
			if currentAgent == "*" {
				allows = append(allows, baseURL+val)
			}

		case "disallow":
			if currentAgent == "*" {
				disallows = append(disallows, baseURL+val)
			}
		}
	}

	return allows, disallows, nil
}
