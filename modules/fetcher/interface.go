package fetcher

type FetcherInterface interface {
	Fetch(url string) ([]byte, error)
}
