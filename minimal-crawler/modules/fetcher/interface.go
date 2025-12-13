package fetcher

type FetcherInterface interface {
	Fetch(url string, timeout float32) ([]byte, error)
	CloseBrowser()
}
