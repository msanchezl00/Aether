package fetcher

type FetcherInterface interface {
	Fetch(url string) (string, error)
}
