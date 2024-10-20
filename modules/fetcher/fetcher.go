package fetcher

import (
	"errors"
	config "minimal-crawler/modules/config"
	"minimal-crawler/utils"
)

type Service struct {
	FetcherConfig config.FetcherConfig
}

func (s *Service) Fetch(url string) (string, error) {
	htmlRaw, err := utils.GetRequest(url)
	if err != nil {
		return "", err
	}
	return string(htmlRaw), nil
}
func (s *Service) FetchRendered(url string) (string, error) {
	// renderizar el html de una pagina con un webdriver
	// con esto se puede obtener informacion de las paginas
	// que son dinamicas asi se ejcutara el js y se retornara
	// el html con toda la informacion extra que puede dar js
	return "", errors.New("not implemented function for render html")
}
