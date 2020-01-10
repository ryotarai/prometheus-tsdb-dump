package writer

import (
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
)

type Writer interface {
	Write(*labels.Labels, []int64, []float64) error
}

func NewWriter(format string) (Writer, error) {
	switch format {
	case "victoriametrics":
		return NewVictoriaMetricsWriter()
	}
	return nil, fmt.Errorf("invalid format: %s", format)
}
