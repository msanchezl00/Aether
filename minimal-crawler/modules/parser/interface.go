package parser

import (
	"minimal-crawler/models"
)

type ParserInterface interface {
	Parse(htmlUTF8 []byte) (models.ContentPayload, error)
}
