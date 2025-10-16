package indexer

import (
	"context"
	"encoding/json"
	models "minimal-indexer/Models"
	config "minimal-indexer/modules/config"
	"minimal-indexer/modules/consumer"
	"minimal-indexer/modules/storage"
	"minimal-indexer/modules/transformer"
	"minimal-indexer/utils"
	"sync"

	"github.com/segmentio/kafka-go"
)

var pool chan struct{}
var wg sync.WaitGroup

type Handler struct {
	IndexerConfig      config.IndexerConfig
	TransformerService transformer.TransformerInterface
	StorageService     storage.StorageInterface
	ConsumerService    consumer.ConsumerInterface
}

func (h *Handler) InitIndexer(ctx context.Context) error {

	// los defer se resuelven el LIFO
	defer config.Logger.Info("indexer finished successfully")
	// proceso padre espera a que las goroutines mueran
	defer wg.Wait()

	pool = make(chan struct{}, h.IndexerConfig.Workers)

	config.Logger.Infof("indexer started with %d workers", h.IndexerConfig.Workers)

	// consumer loop
	wg.Add(1)
	go func() {
		err := h.ConsumerService.Consumer(ctx, func(m kafka.Message) {
			var payload models.KafkaCrawlerPayload
			if err := json.Unmarshal(m.Value, &payload); err != nil {
				config.Logger.Errorf("error parseando payload: %v", err)
				return
			}

			pool <- struct{}{} // ocupa slot ya que va a agregar una goroutine
			wg.Add(1)
			go func(p models.KafkaCrawlerPayload) {
				defer wg.Done()
				defer func() { <-pool }() // libera slot ya que esa goroutine ha acabado su trabajo
				h.Indexer(p)
			}(payload)
		})
		if err != nil {
			config.Logger.Errorf("consumer error: %v", err)
		}
	}()

	return nil
}

func (h *Handler) Indexer(payload models.KafkaCrawlerPayload) {

	// se transforma la informacion recibida de los crawlers a un formato apto para el almacenamiento en HDFS
	payloadProcessed, err := h.TransformerService.Transform(payload)
	if err != nil {
		config.Logger.Errorf("error transform payload: %v", err)
	}

	// se almacena la informacion transformada en Kafka para que luego Kafka Connect la lleve a HDFS
	err = h.StorageService.KafkaStorage(utils.BuildPayloadAvro(payloadProcessed), 0)
	if err != nil {
		config.Logger.Errorf("error storage payload: %v", err)
	}
}
