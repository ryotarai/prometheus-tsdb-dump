package main

import (
	"flag"
	"fmt"
	"github.com/ryotarai/prometheus-tsdb-dump/pkg/writer"
	"log"
	"math"
	"os"

	gokitlog "github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

func main() {
	blockPath := flag.String("block", "", "Path to block directory")
	labelKey := flag.String("label-key", "", "")
	labelValue := flag.String("label-value", "", "")
	minTimestamp := flag.Int64("min-timestamp", 0, "min of timestamp of datapoints to be dumped; unix time in msec")
	maxTimestamp := flag.Int64("max-timestamp", math.MaxInt64, "min of timestamp of datapoints to be dumped; unix time in msec")
	format := flag.String("format", "victoriametrics", "")
	flag.Parse()

	if *blockPath == "" {
		log.Fatal("-block argument is required")
	}

	if err := run(*blockPath, *labelKey, *labelValue, *format, *minTimestamp, *maxTimestamp); err != nil {
		log.Fatalf("error: %s", err)
	}
}

func run(blockPath string, labelKey string, labelValue string, outFormat string, minTimestamp int64, maxTimestamp int64) error {
	wr, err := writer.NewWriter(outFormat)

	logger := gokitlog.NewLogfmtLogger(os.Stderr)

	block, err := tsdb.OpenBlock(logger, blockPath, chunkenc.NewPool())
	if err != nil {
		return errors.Wrap(err, "tsdb.OpenBlock")
	}

	indexr, err := block.Index()
	if err != nil {
		return errors.Wrap(err, "block.Index")
	}
	defer indexr.Close()

	chunkr, err := block.Chunks()
	if err != nil {
		return errors.Wrap(err, "block.Chunks")
	}
	defer chunkr.Close()

	postings, err := indexr.Postings(labelKey, labelValue)
	if err != nil {
		return errors.Wrap(err, "indexr.Postings")
	}

	var it chunkenc.Iterator
	for postings.Next() {
		ref := postings.At()
		lset := labels.Labels{}
		chks := []chunks.Meta{}
		if err := indexr.Series(ref, &lset, &chks); err != nil {
			return errors.Wrap(err, "indexr.Series")
		}

		for _, meta := range chks {
			chunk, err := chunkr.Chunk(meta.Ref)
			if err != nil {
				return errors.Wrap(err, "chunkr.Chunk")
			}

			var timestamps []int64
			var values []float64

			it := chunk.Iterator(it)
			for it.Next() {
				t, v := it.At()
				if math.IsNaN(v) {
					continue
				}
				if t < minTimestamp || maxTimestamp < t {
					continue
				}
				timestamps = append(timestamps, t)
				values = append(values, v)
			}
			if it.Err() != nil {
				return errors.Wrap(err, "iterator.Err")
			}

			if len(timestamps) == 0 {
				continue
			}

			if err := wr.Write(&lset, timestamps, values); err != nil {
				return errors.Wrap(err, fmt.Sprintf("Writer.Write(%v, %v, %v)", lset, timestamps, values))
			}
		}
	}

	if postings.Err() != nil {
		return errors.Wrap(err, "postings.Err")
	}

	return nil
}
