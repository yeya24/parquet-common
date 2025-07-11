// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {
	ts := promqltest.LoadedStorage(t, `
load 1m
	float_only{env="prod"} 5 2+3x200
    float_histogram_conversion{env="prod"} 5 2+3x2 _ stale {{schema:1 sum:3 count:22 buckets:[5 10 7]}}x200
	http_requests_histogram{job="api-server", instance="3", group="canary"} {{schema:2 count:4 sum:10 buckets:[1 0 0 0 1 0 0 1 1]}}x200
    histogram_with_reset_bucket{le="1"} 1  3  9
    histogram_with_reset_bucket{le="2"} 3  3  9
    histogram_with_reset_bucket{le="4"} 8  5 12
    histogram_with_reset_bucket{le="8"} 10  6 18
    histogram_with_reset_sum{}          36 16 61

load_with_nhcb 1m
    histogram_over_time_bucket{le="0"} 0 1 3 9
    histogram_over_time_bucket{le="1"} 2 3 3 9
    histogram_over_time_bucket{le="2"} 3 8 5 10
    histogram_over_time_bucket{le="4"} 3 10 6 18
`)
	require.NotNil(t, ts)

	// Manually append some histogram as the promqltest only generates float histograms
	app := ts.Head().Appender(context.Background())
	generateHistogram(t, app)
	require.NoError(t, app.Commit())

	mint, maxt := ts.Head().MinTime(), ts.Head().MaxTime()
	sb := NewBuilder(mint, maxt, (time.Minute * 60).Milliseconds())
	s, err := sb.Build()
	maxSamplesPerChunk := 30
	enc := NewPrometheusParquetChunksEncoder(s, maxSamplesPerChunk)
	dec := NewPrometheusParquetChunksDecoder(chunkenc.NewPool())

	require.NoError(t, err)

	indexr, err := ts.Head().Index()
	require.NoError(t, err)
	cr, err := ts.Head().Chunks()
	require.NoError(t, err)
	n, v := index.AllPostingsKey()
	p, err := indexr.Postings(context.Background(), n, v)
	require.NoError(t, err)
	chks := []chunks.Meta{}
	builder := labels.ScratchBuilder{}

	for p.Next() {
		require.NoError(t, indexr.Series(p.At(), &builder, &chks))
		totalSamples := 0
		decodedSamples := 0
		for i, chk := range chks {
			c, _, err := cr.ChunkOrIterable(chk)
			require.NoError(t, err)
			chks[i].Chunk = c
			totalSamples += c.NumSamples()
		}
		decodedChunksByTime, err := enc.Encode(storage.NewListChunkSeriesIterator(chks...))
		require.NoError(t, err)
		for _, chunksByTime := range decodedChunksByTime {
			decodedChunkMeta, err := dec.Decode(chunksByTime, mint, maxt)
			require.NoError(t, err)
			for _, decodedChunk := range decodedChunkMeta {
				require.LessOrEqual(t, decodedChunk.Chunk.NumSamples(), maxSamplesPerChunk)
				decodedSamples += decodedChunk.Chunk.NumSamples()
			}
		}
		require.Equal(t, totalSamples, decodedSamples)
	}
}

func generateHistogram(t *testing.T, app storage.Appender) {
	for i := 0; i < 20; i++ {
		_, err := app.AppendHistogram(0, labels.FromStrings(labels.MetricName, "histogram"), int64(i), tsdbutil.GenerateTestCustomBucketsHistogram(int64(i%10)), nil)
		require.NoError(t, err)
	}
}
