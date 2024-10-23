package utils

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/qiniu/iconv"
)

func GetRequest(url string) ([]byte, error) {
	// peticion GET HTTP para obtener el html
	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	// Check del codigo devuelto, si todo va bien deberia ser 200
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	// Se lee el body de la rquest y se pasa a un array de bytes
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	// Obtener el encabezado Content-Type
	contentType := response.Header.Get("Content-Type")
	if strings.Contains(contentType, "charset=") {
		// Extraer la codificación de caracteres
		parts := strings.Split(contentType, "charset=")
		if len(parts) > 1 {
			charset := strings.TrimSpace(parts[1])
			// Convertir de ISO-8859-1 a UTF-8 usando iconv si es necesario
			if strings.EqualFold(charset, "iso-8859-1") {
				return encodeUTF8Format(body)
			}
		}
	}

	return body, nil
}

// convierte un cuerpo en ISO-8859-1 a UTF-8 usando iconvcon
func encodeUTF8Format(data []byte) ([]byte, error) {
	cd, err := iconv.Open("UTF-8", "ISO-8859-1")
	if err != nil {
		return nil, err
	}
	defer cd.Close()

	// Convertir el contenido
	output := make([]byte, len(data)*2) // Reservar espacio suficiente
	_, n, err := cd.Conv(output, data)
	if err != nil {
		return nil, err
	}

	// lee el output desde el principio hasta el byte n
	// ya que la codificacion a utf8 sera igual o menor que ISO
	return output[:n], nil
}
