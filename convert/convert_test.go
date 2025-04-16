package convert

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
)

func Test_Convert_TSDB(t *testing.T) {
	ctx := context.Background()

	tc := []struct {
		dataColDurationMs      time.Duration
		step                   time.Duration
		numberOfSamples        int
		expectedNumberOfChunks int
		expectedPointsPerChunk int
	}{
		{
			dataColDurationMs:      time.Hour,
			step:                   time.Hour,
			numberOfSamples:        3,
			expectedNumberOfChunks: 3,
			expectedPointsPerChunk: 1,
		},
		{
			dataColDurationMs:      time.Hour,
			step:                   time.Hour,
			numberOfSamples:        48,
			expectedNumberOfChunks: 48,
			expectedPointsPerChunk: 1,
		},
		{
			dataColDurationMs:      8 * time.Hour,
			step:                   time.Hour / 2,
			numberOfSamples:        10,
			expectedNumberOfChunks: 1,
			expectedPointsPerChunk: 10,
		},
		{
			dataColDurationMs:      8 * time.Hour,
			step:                   time.Hour / 2,
			numberOfSamples:        32,
			expectedNumberOfChunks: 2,
			expectedPointsPerChunk: 16,
		},
	}

	for _, tt := range tc {
		t.Run(fmt.Sprintf("dataColDurationMs:%v,step:%v,numberOfSamples:%v", tt.dataColDurationMs.Hours(), tt.step.Seconds(), tt.numberOfSamples), func(t *testing.T) {
			st := teststorage.New(t)
			t.Cleanup(func() { st.Close() })

			app := st.Appender(ctx)
			now := time.Now()
			seriesHash := make(map[uint64]struct{})
			for i := 0; i != 1_000; i++ {
				for j := 0; j < tt.numberOfSamples; j++ {
					lbls := labels.FromStrings("__name__", "foo", "bar", fmt.Sprintf("%d", 2*i))
					seriesHash[lbls.Hash()] = struct{}{}
					_, err := app.Append(0, lbls, TimeToMilliseconds(now.Add(tt.step*time.Duration(j))), float64(i))
					require.NoError(t, err)
				}
			}

			require.NoError(t, app.Commit())

			h := st.DB.Head()
			rr, err := newTsdbRowReader(ctx, tt.dataColDurationMs, []Convertible{h})
			require.NoError(t, err)

			defer func() { _ = rr.Close() }()

			buf := make([]parquet.Row, 100)
			chunksDecoder := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
			total := 0

			for {
				n, _ := rr.ReadRows(buf)
				if n == 0 {
					break
				}

				total += n
				series, chunks, err := rowToSeries(rr.schema, chunksDecoder, buf[:n])
				require.NoError(t, err)
				require.Len(t, series, n)
				for i, s := range series {
					require.Contains(t, seriesHash, s.Hash())
					require.Len(t, chunks[i], tt.expectedNumberOfChunks)
					totalSamples := 0
					for _, c := range chunks[i] {
						require.Equal(t, tt.expectedPointsPerChunk, c.Chunk.NumSamples())
						totalSamples += c.Chunk.NumSamples()
					}
					require.Equal(t, tt.numberOfSamples, totalSamples)
				}
			}

			require.Equal(t, st.DB.Head().NumSeries(), uint64(total))
		})
	}
}

// TimeToMilliseconds returns the input time as milliseconds, using the same
// formula used by Prometheus in order to get the same timestamp when asserting
// on query results. The formula we're mimicking here is Prometheus parseTime().
// See: https://github.com/prometheus/prometheus/blob/df80dc4d3970121f2f76cba79050983ffb3cdbb0/web/api/v1/api.go#L1690-L1694
func TimeToMilliseconds(t time.Time) int64 {
	// Convert to seconds.
	sec := float64(t.Unix()) + float64(t.Nanosecond())/1e9

	// Parse seconds.
	s, ns := math.Modf(sec)

	// Round nanoseconds part.
	ns = math.Round(ns*1000) / 1000

	// Convert to millis.
	return (int64(s) * 1e3) + (int64(ns * 1e3))
}

func rowToSeries(s *schema.TSDBSchema, dec *schema.PrometheusParquetChunksDecoder, rows []parquet.Row) ([]labels.Labels, [][]chunks.Meta, error) {
	cols := s.Schema.Columns()
	b := labels.NewScratchBuilder(10)
	series := make([]labels.Labels, len(rows))
	chunksMetas := make([][]chunks.Meta, len(rows))

	for i, row := range rows {
		b.Reset()
		for colIdx, colVal := range row {
			col := cols[colIdx][0]
			label, ok := schema.ExtractLabelFromColumn(col)
			if ok {
				b.Add(label, colVal.String())
			}

			if schema.IsDataColumn(col) {
				c, err := dec.Decode(colVal.ByteArray(), 0, math.MaxInt64)
				if err != nil {
					return nil, nil, err
				}
				chunksMetas[i] = append(chunksMetas[i], c...)
			}
		}
		series[i] = b.Labels()
	}

	return series, chunksMetas, nil
}
