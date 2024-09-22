package fetcher

type FetcherService interface {
	Fetch()
	FetchHtml()
	FetchMetadata()
	FetchLinks()
	FetchText()
	FetchStructures()
	FetchImages()
}
