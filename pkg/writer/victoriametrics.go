package writer

import (
	"encoding/json"
	"github.com/prometheus/prometheus/pkg/labels"
	"os"
)

type VictoriaMetricsWriter struct {
}

func NewVictoriaMetricsWriter() (*VictoriaMetricsWriter, error) {
	return &VictoriaMetricsWriter{}, nil
}

type victoriaMetricsLine struct {
	Metric     map[string]string `json:"metric"`
	Values     []float64         `json:"values"`
	Timestamps []int64           `json:"timestamps"`
}

func (w *VictoriaMetricsWriter) Write(labels *labels.Labels, timestamps []int64, values []float64) error {
	metric := map[string]string{}
	for _, l := range *labels {
		metric[l.Name] = l.Value
	}

	enc := json.NewEncoder(os.Stdout)
	err := enc.Encode(victoriaMetricsLine{
		Metric:     metric,
		Values:     values,
		Timestamps: timestamps,
	})
	if err != nil {
		return err
	}
	return nil
}
