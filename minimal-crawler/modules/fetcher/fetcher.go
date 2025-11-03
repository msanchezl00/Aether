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
	// Fetch normal primero
	htmlUTF8, err := utils.GetRequest(url, timeout)
	if err != nil {
		return nil, err
	}

	// Limpieza básica del HTML para análisis
	htmlLower := bytes.ToLower(htmlUTF8)

	// Heurística mejorada
	isDynamic := false

	// Contenido mínimo
	if len(htmlUTF8) < 1000 {
		isDynamic = true
	}

	// Body vacío
	if bytes.Contains(htmlLower, []byte("<body></body>")) ||
		bytes.Contains(htmlLower, []byte("<body/>")) {
		isDynamic = true
	}

	// Solo scripts o comentarios
	bodyStart := bytes.Index(htmlLower, []byte("<body"))
	bodyEnd := bytes.Index(htmlLower, []byte("</body>"))
	if bodyStart != -1 && bodyEnd != -1 && bodyEnd > bodyStart {
		bodyContent := htmlLower[bodyStart:bodyEnd]
		// Si tiene pocos caracteres visibles
		visibleChars := bytes.Count(bodyContent, []byte("a")) + bytes.Count(bodyContent, []byte("p"))
		if visibleChars < 10 {
			isDynamic = true
		}
	}

	// Indicadores de SPA (React, Angular, Vue...)
	if bytes.Contains(htmlLower, []byte("id=\"root\"")) ||
		bytes.Contains(htmlLower, []byte("id=\"app\"")) ||
		bytes.Contains(htmlLower, []byte("ng-app")) {
		isDynamic = true
	}

	// usar FetchRendered con headless browser
	if isDynamic {
		rendered, err := FetchRendered(url, timeout)
		if err != nil {
			return nil, err
		}
		return rendered, nil
	}

	// Si no, retornar fetch normal
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
