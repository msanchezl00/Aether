package fetcher

type FetcherInterface interface {
	Fetch()
	FetchHtml()
	FetchMetadata()
	FetchLinks()
	FetchText()
	FetchStructures()
	FetchImages()
}
