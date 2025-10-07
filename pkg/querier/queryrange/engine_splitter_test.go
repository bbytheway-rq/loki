package queryrange

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
	"github.com/grafana/loki/v3/pkg/storage/chunk/cache/resultscache"
)

func TestEngineSplitter_CreateOverlappingSplits(t *testing.T) {
	now := time.Now()
	v2Range := EngineTimeRange{
		Start: now.Add(-2 * 24 * time.Hour), // 2 days ago
		End:   now.Add(-2 * time.Hour),      // 2 hours ago
	}

	splitter := &engineSplitter{v2EngineRange: v2Range}

	tests := []struct {
		name            string
		start           time.Time
		end             time.Time
		expectedV1Times [][]time.Time // [][]{start, end} for V1 requests
		expectedV2Times []time.Time   // {start, end} for V2 request
	}{
		// Original test cases
		{
			name:            "Query entirely within V2 range",
			start:           now.Add(-24 * time.Hour), // 1 day ago
			end:             now.Add(-3 * time.Hour),  // 3 hours ago
			expectedV1Times: [][]time.Time{},
			expectedV2Times: []time.Time{now.Add(-24 * time.Hour), now.Add(-3 * time.Hour)},
		},
		{
			name:            "Query overlaps V2 range - before and within",
			start:           now.Add(-3 * 24 * time.Hour), // 3 days ago
			end:             now.Add(-24 * time.Hour),     // 1 day ago
			expectedV1Times: [][]time.Time{{now.Add(-3 * 24 * time.Hour), now.Add(-2 * 24 * time.Hour)}},
			expectedV2Times: []time.Time{now.Add(-2 * 24 * time.Hour), now.Add(-24 * time.Hour)},
		},
		{
			name:            "Query overlaps V2 range - within and after",
			start:           now.Add(-24 * time.Hour), // 1 day ago
			end:             now.Add(-time.Hour),      // 1 hour ago
			expectedV1Times: [][]time.Time{{now.Add(-2 * time.Hour), now.Add(-time.Hour)}},
			expectedV2Times: []time.Time{now.Add(-24 * time.Hour), now.Add(-2 * time.Hour)},
		},
		{
			name:  "Query spans entire V2 range",
			start: now.Add(-3 * 24 * time.Hour), // 3 days ago
			end:   now.Add(-time.Hour),          // 1 hour ago
			expectedV1Times: [][]time.Time{
				{now.Add(-3 * 24 * time.Hour), now.Add(-2 * 24 * time.Hour)},
				{now.Add(-2 * time.Hour), now.Add(-time.Hour)},
			},
			expectedV2Times: []time.Time{now.Add(-2 * 24 * time.Hour), now.Add(-2 * time.Hour)},
		},

		// Boundary edge cases
		{
			name:            "Query touches V2 range start exactly",
			start:           now.Add(-2 * 24 * time.Hour), // Exactly at V2 start
			end:             now.Add(-24 * time.Hour),
			expectedV1Times: [][]time.Time{},
			expectedV2Times: []time.Time{now.Add(-2 * 24 * time.Hour), now.Add(-24 * time.Hour)},
		},
		{
			name:            "Query touches V2 range end exactly",
			start:           now.Add(-24 * time.Hour),
			end:             now.Add(-2 * time.Hour), // Exactly at V2 end
			expectedV1Times: [][]time.Time{},
			expectedV2Times: []time.Time{now.Add(-24 * time.Hour), now.Add(-2 * time.Hour)},
		},

		// Very small overlaps
		{
			name:            "Query with minimal overlap at V2 start",
			start:           now.Add(-2*24*time.Hour - time.Minute), // 1 minute before V2 start
			end:             now.Add(-2*24*time.Hour + time.Minute), // 1 minute after V2 start
			expectedV1Times: [][]time.Time{{now.Add(-2*24*time.Hour - time.Minute), now.Add(-2 * 24 * time.Hour)}},
			expectedV2Times: []time.Time{now.Add(-2 * 24 * time.Hour), now.Add(-2*24*time.Hour + time.Minute)},
		},
		{
			name:            "Query with minimal overlap at V2 end",
			start:           now.Add(-2*time.Hour - time.Minute), // 1 minute before V2 end
			end:             now.Add(-2*time.Hour + time.Minute), // 1 minute after V2 end
			expectedV1Times: [][]time.Time{{now.Add(-2 * time.Hour), now.Add(-2*time.Hour + time.Minute)}},
			expectedV2Times: []time.Time{now.Add(-2*time.Hour - time.Minute), now.Add(-2 * time.Hour)},
		},

		// Query exactly matches V2 range
		{
			name:            "Query exactly matches V2 range",
			start:           now.Add(-2 * 24 * time.Hour), // Exactly V2 start
			end:             now.Add(-2 * time.Hour),      // Exactly V2 end
			expectedV1Times: [][]time.Time{},
			expectedV2Times: []time.Time{now.Add(-2 * 24 * time.Hour), now.Add(-2 * time.Hour)},
		},

		// Query contains V2 range (V2 is subset)
		{
			name:  "Query contains entire V2 range",
			start: now.Add(-3 * 24 * time.Hour), // Before V2 start
			end:   now.Add(-time.Hour),          // After V2 end
			expectedV1Times: [][]time.Time{
				{now.Add(-3 * 24 * time.Hour), now.Add(-2 * 24 * time.Hour)},
				{now.Add(-2 * time.Hour), now.Add(-time.Hour)},
			},
			expectedV2Times: []time.Time{now.Add(-2 * 24 * time.Hour), now.Add(-2 * time.Hour)},
		},

		// Microsecond precision edge cases
		{
			name:            "Query with microsecond precision boundaries",
			start:           now.Add(-2 * 24 * time.Hour).Add(time.Microsecond),
			end:             now.Add(-2 * time.Hour).Add(-time.Microsecond),
			expectedV1Times: [][]time.Time{},
			expectedV2Times: []time.Time{
				now.Add(-2 * 24 * time.Hour).Add(time.Microsecond),
				now.Add(-2 * time.Hour).Add(-time.Microsecond),
			},
		},

		// Multiple small gaps
		{
			name:  "Query with multiple small gaps around V2 range",
			start: now.Add(-2*24*time.Hour - 2*time.Hour), // 2 hours before V2 start
			end:   now.Add(-2*time.Hour + 2*time.Hour),    // 2 hours after V2 end
			expectedV1Times: [][]time.Time{
				{now.Add(-2*24*time.Hour - 2*time.Hour), now.Add(-2 * 24 * time.Hour)},
				{now.Add(-2 * time.Hour), now.Add(-2*time.Hour + 2*time.Hour)},
			},
			expectedV2Times: []time.Time{now.Add(-2 * 24 * time.Hour), now.Add(-2 * time.Hour)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a simple mock request for testing
			mockReq := &mockRequest{
				StartTs: tt.start,
				EndTs:   tt.end,
				Query:   "test query",
				Step:    1000, // 1 second step
			}

			reqs := splitter.split(mockReq, tt.start, tt.end, v2Range)

			// Count V1 and V2 requests and validate timestamps
			v1Times := [][]time.Time{}
			var v2Times []time.Time

			for _, req := range reqs {
				reqStart := req.req.GetStart()
				reqEnd := req.req.GetEnd()

				// Verify all requests have valid time ranges
				assert.True(t, reqStart.Before(reqEnd), "Request should have valid time range: start=%v, end=%v", reqStart, reqEnd)

				if req.isV2Engine {
					v2Times = []time.Time{reqStart, reqEnd}
				} else {
					v1Times = append(v1Times, []time.Time{reqStart, reqEnd})
				}
			}

			// Validate counts by checking slice lengths
			assert.Equal(t, len(tt.expectedV1Times), len(v1Times), "V1 request count")
			assert.Equal(t, len(tt.expectedV2Times) > 0, len(v2Times) > 0, "V2 request count")

			// Validate V1 request timestamps
			for i, expectedTimes := range tt.expectedV1Times {
				if i < len(v1Times) {
					assert.Equal(t, expectedTimes[0], v1Times[i][0], "V1 request %d start time", i)
					assert.Equal(t, expectedTimes[1], v1Times[i][1], "V1 request %d end time", i)
				}
			}

			// Validate V2 request timestamps
			if len(tt.expectedV2Times) > 0 {
				assert.Equal(t, tt.expectedV2Times[0], v2Times[0], "V2 request start time")
				assert.Equal(t, tt.expectedV2Times[1], v2Times[1], "V2 request end time")
			}
		})
	}
}

func TestEngineSplitter_StepAlignment(t *testing.T) {
	now := time.Now()
	v2Range := EngineTimeRange{
		Start: now.Add(-2 * 24 * time.Hour), // 2 days ago
		End:   now.Add(-2 * time.Hour),      // 2 hours ago
	}

	splitter := &engineSplitter{v2EngineRange: v2Range}

	tests := []struct {
		name            string
		start           time.Time
		end             time.Time
		step            int64         // in milliseconds
		expectedV1Times [][]time.Time // [][]{start, end} for V1 requests
		expectedV2Times []time.Time   // {start, end} for V2 request
		description     string
	}{
		{
			name:            "3 minute step alignment - query within V2 range",
			start:           now.Add(-24 * time.Hour).Add(1 * time.Minute), // 1 minute offset from 3m boundary
			end:             now.Add(-3 * time.Hour).Add(2 * time.Minute),  // 2 minute offset from 3m boundary
			step:            3 * 60 * 1000,                                 // 3 minutes in milliseconds
			expectedV1Times: [][]time.Time{},
			expectedV2Times: []time.Time{
				now.Add(-24 * time.Hour).Add(1 * time.Minute).Truncate(3 * time.Minute),                     // Rounded down to 3m boundary
				now.Add(-3 * time.Hour).Add(2 * time.Minute).Truncate(3 * time.Minute).Add(3 * time.Minute), // Rounded up to next 3m boundary
			},
			description: "Start should be rounded down, end should be rounded up to 3-minute boundaries",
		},
		{
			name:  "5 minute step alignment - query spans V2 range",
			start: now.Add(-3 * 24 * time.Hour).Add(1 * time.Minute), // Before V2, with offset
			end:   now.Add(-time.Hour).Add(2 * time.Minute),          // After V2, with offset
			step:  5 * 60 * 1000,                                     // 5 minutes in milliseconds
			expectedV1Times: [][]time.Time{
				{
					now.Add(-3 * 24 * time.Hour).Add(1 * time.Minute).Truncate(5 * time.Minute), // Rounded down
					now.Add(-2 * 24 * time.Hour).Truncate(5 * time.Minute).Add(5 * time.Minute), // V2 start (rounded up)
				},
				{
					now.Add(-2 * time.Hour).Truncate(5 * time.Minute),                                       // V2 end (rounded down)
					now.Add(-time.Hour).Add(2 * time.Minute).Truncate(5 * time.Minute).Add(5 * time.Minute), // Rounded up
				},
			},
			expectedV2Times: []time.Time{
				now.Add(-3 * 24 * time.Hour).Add(1 * time.Minute).Truncate(5 * time.Minute), // Query start (rounded down)
				now.Add(-2 * time.Hour).Truncate(5 * time.Minute),                           // V2 end (rounded down)
			},
			description: "Should create 3 requests: V1 before, V2 within, V1 after, all aligned to 5-minute boundaries",
		},
		{
			name:            "1 minute step alignment - exact boundaries",
			start:           now.Add(-24 * time.Hour), // Exactly on minute boundary
			end:             now.Add(-3 * time.Hour),  // Exactly on minute boundary
			step:            1 * 60 * 1000,            // 1 minute in milliseconds
			expectedV1Times: [][]time.Time{},
			expectedV2Times: []time.Time{
				now.Add(-24 * time.Hour).Truncate(time.Minute),                 // Already aligned
				now.Add(-3 * time.Hour).Truncate(time.Minute).Add(time.Minute), // Rounded up to next minute
			},
			description: "Times already on boundaries should remain unchanged",
		},
		{
			name:            "30 second step alignment - sub-minute precision",
			start:           now.Add(-24 * time.Hour).Add(15 * time.Second), // 15 seconds offset
			end:             now.Add(-3 * time.Hour).Add(45 * time.Second),  // 45 seconds offset
			step:            30 * 1000,                                      // 30 seconds in milliseconds
			expectedV1Times: [][]time.Time{},
			expectedV2Times: []time.Time{
				now.Add(-24 * time.Hour).Add(15 * time.Second).Truncate(30 * time.Second),                      // Rounded down to 30s boundary
				now.Add(-3 * time.Hour).Add(45 * time.Second).Truncate(30 * time.Second).Add(30 * time.Second), // Rounded up to next 30s boundary
			},
			description: "Should align to 30-second boundaries",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockReq := &mockRequest{
				StartTs: tt.start,
				EndTs:   tt.end,
				Query:   "test query",
				Step:    tt.step,
			}

			reqs := splitter.split(mockReq, tt.start, tt.end, v2Range)

			t.Logf("Test: %s", tt.description)
			t.Logf("Original query: %v to %v (step: %dms)", tt.start, tt.end, tt.step)

			// Count V1 and V2 requests and validate timestamps
			v1Times := [][]time.Time{}
			var v2Times []time.Time

			for _, req := range reqs {
				reqStart := req.req.GetStart()
				reqEnd := req.req.GetEnd()
				engineType := "V1"
				if req.isV2Engine {
					engineType = "V2"
				}

				t.Logf("  %s request: %v to %v", engineType, reqStart, reqEnd)

				// Verify all requests have valid time ranges
				assert.True(t, reqStart.Before(reqEnd), "Request should have valid time range: start=%v, end=%v", reqStart, reqEnd)

				// Check that times are aligned to step boundary
				startNs := reqStart.UnixNano()
				stepNs := tt.step * 1e6
				assert.Equal(t, int64(0), startNs%stepNs, "%s request start time not aligned to step boundary: %v (step: %dms)", engineType, reqStart, tt.step)

				endNs := reqEnd.UnixNano()
				assert.Equal(t, int64(0), endNs%stepNs, "%s request end time not aligned to step boundary: %v (step: %dms)", engineType, reqEnd, tt.step)

				if req.isV2Engine {
					v2Times = []time.Time{reqStart, reqEnd}
				} else {
					v1Times = append(v1Times, []time.Time{reqStart, reqEnd})
				}
			}

			// Validate counts by checking slice lengths
			assert.Equal(t, len(tt.expectedV1Times), len(v1Times), "V1 request count")
			assert.Equal(t, len(tt.expectedV2Times) > 0, len(v2Times) > 0, "V2 request count")

			// Validate V1 request timestamps
			for i, expectedTimes := range tt.expectedV1Times {
				if i < len(v1Times) {
					assert.Equal(t, expectedTimes[0], v1Times[i][0], "V1 request %d start time", i)
					assert.Equal(t, expectedTimes[1], v1Times[i][1], "V1 request %d end time", i)
				}
			}

			// Validate V2 request timestamps
			if len(tt.expectedV2Times) > 0 {
				assert.Equal(t, tt.expectedV2Times[0], v2Times[0], "V2 request start time")
				assert.Equal(t, tt.expectedV2Times[1], v2Times[1], "V2 request end time")
			}
		})
	}
}

// mockRequest implements queryrangebase.Request for testing purposes
type mockRequest struct {
	StartTs time.Time
	EndTs   time.Time
	Query   string
	Step    int64
}

func (m *mockRequest) GetStart() time.Time { return m.StartTs }
func (m *mockRequest) GetEnd() time.Time   { return m.EndTs }
func (m *mockRequest) GetQuery() string    { return m.Query }
func (m *mockRequest) GetStep() int64      { return m.Step }
func (m *mockRequest) GetCachingOptions() resultscache.CachingOptions {
	return resultscache.CachingOptions{}
}
func (m *mockRequest) WithStartEnd(start, end time.Time) queryrangebase.Request {
	return &mockRequest{
		StartTs: start,
		EndTs:   end,
		Query:   m.Query,
		Step:    m.Step,
	}
}
func (m *mockRequest) WithQuery(query string) queryrangebase.Request {
	return &mockRequest{
		StartTs: m.StartTs,
		EndTs:   m.EndTs,
		Query:   query,
		Step:    m.Step,
	}
}
func (m *mockRequest) LogToSpan(sp trace.Span) {}
func (m *mockRequest) ProtoMessage()           {}
func (m *mockRequest) Reset()                  {}
func (m *mockRequest) String() string          { return m.Query }
