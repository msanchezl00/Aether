package fetcher

type FetcherInterface interface {
	Fetch(url string) (string, error)
	FetchHtml()
	FetchMetadata()
	FetchLinks()
	FetchText()
	FetchStructures()
	FetchImages()
}
