package fetcher

import (
	"bytes"
	config "minimal-crawler/modules/config"
	"minimal-crawler/utils"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
)

type Service struct {
	FetcherConfig config.FetcherConfig
}

func (s *Service) Fetch(url string, timeout float32) ([]byte, error) {
	// Primero intento fetch normal
	htmlUTF8, err := utils.GetRequest(url, timeout)
	if err != nil {
		return nil, err
	}

	// Heurística: si el HTML es muy corto o contiene solo scripts, usar renderizado
	if len(htmlUTF8) < 500 || bytes.Contains(htmlUTF8, []byte("<script")) {
		rendered, err := FetchRendered(url, timeout)
		if err != nil {
			return nil, err
		}
		return rendered, nil
	}

	return htmlUTF8, nil
}

func FetchRendered(url string, timeout float32) ([]byte, error) {
	// Lanza browser headless
	browser := rod.New().ControlURL(launcher.New().Headless(true).MustLaunch()).MustConnect()
	defer browser.MustClose()

	// Abre página
	page := browser.MustPage(url)

	// Espera que el body esté visible
	page.MustElement("body").MustWaitVisible()

	// Timeout opcional: espera JS dinámico
	time.Sleep(time.Duration(timeout * float32(time.Second)))

	// Obtén HTML completo
	html, err := page.HTML()
	if err != nil {
		return nil, err
	}

	return []byte(html), nil
}
