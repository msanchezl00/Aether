package utils

import (
	"fmt"
	"io"
	"net/http"
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

	return body, nil
}
