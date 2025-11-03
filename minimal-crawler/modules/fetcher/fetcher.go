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

	// Usar FetchRendered si es página dinámica
	if isDynamic {
		rendered, err := s.FetchRendered(url, timeout)
		if err != nil {
			return nil, err
		}
		return rendered, nil
	}

	// Retornar fetch normal
	return htmlUTF8, nil
}

func (s *Service) FetchRendered(url string, timeout float32) ([]byte, error) {
	// Lanzar browser headless con Rod (usa Chromium automático)
	browser := rod.New().ControlURL(
		launcher.New().
			Headless(true). // Headless
			MustLaunch(),   // Descarga Chromium si no existe
	).MustConnect()
	defer browser.MustClose()

	// Abrir página
	page := browser.MustPage(url)

	// Esperar body renderizado
	page.MustElement("body").MustWaitVisible()

	// Espera opcional para JS dinámico
	time.Sleep(time.Duration(timeout * float32(time.Second)))

	// Obtener HTML completo
	html, err := page.HTML()
	if err != nil {
		return nil, err
	}

	return []byte(html), nil
}
