package fetcher

import (
	core "minimal-crawler/modules/config"
	"minimal-crawler/utils"
)

type Service struct {
	FetcherConfig core.FetcherConfig
}

func (s *Service) Fetch(url string) (string, error) {
	htmlRaw, err := utils.GetRequest(url)
	if err != nil {
		return "", err
	}
	return string(htmlRaw), nil
}
func (s *Service) FetchHtml() {

}
func (s *Service) FetchMetadata() {

}
func (s *Service) FetchLinks() {

}
func (s *Service) FetchText() {

}
func (s *Service) FetchStructures() {

}
func (s *Service) FetchImages() {

}
