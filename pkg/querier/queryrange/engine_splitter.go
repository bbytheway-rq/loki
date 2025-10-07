// Package queryrange provides middleware for splitting queries between V1 and V2 engines.
// The engine splitter analyzes query time ranges and routes them to the appropriate engine
// based on the V2 engine's availability window.
package queryrange

import (
	"context"
	"time"

	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
	"github.com/pkg/errors"
)

// EngineTimeRange defines the time range for the V2 engine
type EngineTimeRange struct {
	Start time.Time
	End   time.Time
}

// engineReqResp represents a request with its result channel
type engineReqResp struct {
	lokiResult
	isV2Engine bool
}

// engineSplitter handles splitting queries between V1 and V2 engines
type engineSplitter struct {
	v2EngineRange EngineTimeRange
	v1Chain       []queryrangebase.Middleware
	merger        queryrangebase.Merger
	next          queryrangebase.Handler
}

// NewEngineSplitMiddleware creates a middleware that splits queries between V1 and V2 engines
func NewEngineSplitMiddleware(
	v2EngineRange EngineTimeRange,
	v1Chain []queryrangebase.Middleware,
	merger queryrangebase.Merger,
) queryrangebase.Middleware {
	return queryrangebase.MiddlewareFunc(func(next queryrangebase.Handler) queryrangebase.Handler {
		return &engineSplitter{
			v2EngineRange: v2EngineRange,
			v1Chain:       v1Chain,
			merger:        merger,
			next:          next,
		}
	})
}

// TODO:
// - apply limits for log queries, consider query direction.
// - handle very small splits
func (e *engineSplitter) Do(ctx context.Context, r queryrangebase.Request) (queryrangebase.Response, error) {
	start := r.GetStart()
	end := r.GetEnd()

	// if query is entirely before or after v2 engine range, process using next handler.
	// end is exclusive for log and metric queries, even if there is boundary overlap use old engine.
	if !end.After(e.v2EngineRange.Start) || start.After(e.v2EngineRange.End) {
		return queryrangebase.MergeMiddlewares(e.v1Chain...).Wrap(e.next).Do(ctx, r)
	}

	inputs := e.split(r, start, end, e.v2EngineRange)

	responses, err := e.Process(ctx, inputs)
	if err != nil {
		return nil, err
	}

	// Merge responses
	return e.merger.MergeResponse(responses...)
}

// alignStartEnd aligns start and end times to step boundaries.
// By default start is rounded down and end is rounded up to the nearest step boundary.
// If shrink is true, start is rounded up and end is rounded down to the nearest step boundary.
func (e *engineSplitter) alignStartEnd(step int64, start, end time.Time, shrink bool) (time.Time, time.Time) {
	stepNs := step * 1e6
	startNs := start.UnixNano()
	endNs := end.UnixNano()

	if mod := startNs % stepNs; mod != 0 {
		if shrink {
			startNs += stepNs - mod // round up
		} else {
			startNs -= mod // round down
		}
	}

	if mod := endNs % stepNs; mod != 0 {
		if shrink {
			endNs -= mod // round down
		} else {
			endNs += stepNs - mod // round up
		}
	}

	return time.Unix(0, startNs), time.Unix(0, endNs)
}

// split splits the request into multiple requests based on the V2 engine time range.
func (e *engineSplitter) split(r queryrangebase.Request, start, end time.Time, v2Engine EngineTimeRange) []*engineReqResp {
	// align query start/end to step boundaries
	start, end = e.alignStartEnd(r.GetStep(), start, end, false)
	// shrink the range to stay within original range.
	v2Start, v2End := e.alignStartEnd(r.GetStep(), v2Engine.Start, v2Engine.End, true)

	// End time is exclusive for metric and log queries.
	// So the split queries can overlap on the boundary.
	// If V2 engine supports metadata queries, those need to include a 1ms gap between splits.
	var reqs []*engineReqResp

	// chunk req before V2 engine range
	if start.Before(v2Start) {
		reqs = append(reqs, &engineReqResp{
			lokiResult: lokiResult{
				req: r.WithStartEnd(start, v2Start),
				ch:  make(chan *packedResp),
			},
			isV2Engine: false,
		})
	}

	// chunk req after V2 engine range
	if end.After(v2End) {
		reqs = append(reqs, &engineReqResp{
			lokiResult: lokiResult{
				req: r.WithStartEnd(v2End, end),
				ch:  make(chan *packedResp),
			},
			isV2Engine: false,
		})
	}

	if end.After(v2Start) {
		v2Start = start
	}
	if end.Before(v2End) {
		v2End = end
	}

	// TODO: req order is important for log queries with a limit.
	return append(reqs, &engineReqResp{
		lokiResult: lokiResult{
			req: r.WithStartEnd(v2Start, v2End),
			ch:  make(chan *packedResp),
		},
		isV2Engine: true,
	})
}

func (e *engineSplitter) processRequest(ctx context.Context, r *engineReqResp) {
	if r.isV2Engine {
		// TODO: Add handler for v2 engine.
		panic("V2 engine handler not implemented")
	}

	resp, err := queryrangebase.MergeMiddlewares(e.v1Chain...).Wrap(e.next).Do(ctx, r.req)

	select {
	case <-ctx.Done():
		return
	case r.ch <- &packedResp{resp, err}:
	}
}

// Process executes engine splits in parallel with proper cancellation
func (e *engineSplitter) Process(ctx context.Context, inputs []*engineReqResp) ([]queryrangebase.Response, error) {
	var responses []queryrangebase.Response
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(errors.New("engine splitter process canceled"))

	// Run all requests in parallel as only have a max of 3 splits.
	for _, r := range inputs {
		go e.processRequest(ctx, r)
	}

	// Collect results and cancel on first error
	for _, x := range inputs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case data := <-x.ch:
			if data.err != nil {
				return nil, data.err
			}
			responses = append(responses, data.resp)
		}
	}

	return responses, nil
}

// NewV2EngineTimeRange creates a V2 engine time range based on current time and configuration
func NewV2EngineTimeRange(now time.Time, v2StartOffset, v2EndOffset time.Duration) EngineTimeRange {
	return EngineTimeRange{
		Start: now.Add(v2StartOffset),
		End:   now.Add(v2EndOffset),
	}
}
