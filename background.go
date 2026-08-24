package restlytics

import (
	"context"
	"strings"
)

type QueueCarrier map[string]any

type JobOptions struct {
	Name, System, Destination string
	Attempt, MaxAttempts      int
	EnqueuedNS                int64
	MessageID, Traceparent    string
}

type CommandOptions struct {
	Name, Traceparent string
}

type ScheduleOptions struct {
	Name, Cron, Traceparent string
}

type EnqueueOptions struct {
	System, Destination, Tracestate string
}

func stableWorkName(value, fallback string) string {
	value = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, strings.TrimSpace(value))
	if value == "" {
		return fallback
	}
	if len(value) > 200 {
		return value[:200]
	}
	return value
}

func (t *Tracer) RunJob(ctx context.Context, options JobOptions, operation func(context.Context) error) (err error) {
	name := stableWorkName(options.Name, "unnamed-job")
	ctx = t.startRoot(ctx, name, options.Traceparent, KindConsumer, CategoryJob, true)
	root := RootSpan(ctx)
	if root != nil {
		root.SetString("restlytics.work.name", name)
		root.SetString("restlytics.job.name", name)
		root.SetString("messaging.system", stableWorkName(options.System, "unknown"))
		root.SetString("messaging.destination.name", stableWorkName(options.Destination, "unknown"))
		root.SetString("messaging.operation.type", "process")
		attempt := options.Attempt
		if attempt < 1 {
			attempt = 1
		}
		root.SetInt("restlytics.job.attempt", int64(attempt))
		if options.MaxAttempts > 0 {
			root.SetInt("restlytics.job.max_attempts", int64(options.MaxAttempts))
		}
		if options.EnqueuedNS > 0 {
			root.SetInt("restlytics.job.enqueued_ns", options.EnqueuedNS)
		}
		if options.MessageID != "" {
			root.SetString("messaging.message.id", stableWorkName(options.MessageID, "unknown"))
		}
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			if root != nil {
				root.SetStatus(StatusError, "")
			}
			t.Finish(ctx)
			panic(recovered)
		}
		if err != nil && root != nil {
			root.SetStatus(StatusError, "")
		}
		t.Finish(ctx)
	}()
	return operation(ctx)
}

func (t *Tracer) RunCommand(ctx context.Context, options CommandOptions, operation func(context.Context) (int, error)) (exitCode int, err error) {
	name := stableWorkName(options.Name, "unnamed-command")
	ctx = t.startRoot(ctx, name, options.Traceparent, KindServer, CategoryCommand, false)
	root := RootSpan(ctx)
	if root != nil {
		root.SetString("restlytics.work.name", name)
		root.SetString("restlytics.command.name", name)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			if root != nil {
				root.SetInt("restlytics.command.exit_code", 1)
				root.SetStatus(StatusError, "")
			}
			t.Finish(ctx)
			panic(recovered)
		}
		if root != nil {
			root.SetInt("restlytics.command.exit_code", int64(exitCode))
			if exitCode != 0 || err != nil {
				root.SetStatus(StatusError, "")
			}
		}
		t.Finish(ctx)
	}()
	return operation(ctx)
}

func (t *Tracer) RunSchedule(ctx context.Context, options ScheduleOptions, operation func(context.Context) error) (err error) {
	name := stableWorkName(options.Name, "unnamed-schedule")
	ctx = t.startRoot(ctx, name, options.Traceparent, KindServer, CategorySchedule, false)
	root := RootSpan(ctx)
	if root != nil {
		root.SetString("restlytics.work.name", name)
		root.SetString("restlytics.schedule.name", name)
		root.SetString("restlytics.schedule.cron", stableWorkName(options.Cron, "unknown"))
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			if root != nil {
				root.SetStatus(StatusError, "")
			}
			t.Finish(ctx)
			panic(recovered)
		}
		if err != nil && root != nil {
			root.SetStatus(StatusError, "")
		}
		t.Finish(ctx)
	}()
	return operation(ctx)
}

func Enqueue(ctx context.Context, options EnqueueOptions, carrier QueueCarrier, operation func(QueueCarrier) error) (err error) {
	st := fromContext(ctx)
	if st == nil {
		return operation(carrier)
	}
	spanID := NewSpanID()
	carrier["__restlytics"] = map[string]string{
		"traceparent": FormatTraceparent(st.traceID, spanID, st.sampled),
	}
	if strings.TrimSpace(options.Tracestate) != "" {
		envelope := carrier["__restlytics"].(map[string]string)
		state := strings.TrimSpace(options.Tracestate)
		if len(state) > 512 {
			state = state[:512]
		}
		envelope["tracestate"] = state
	}
	span := startChildSpan(ctx, "send "+stableWorkName(options.Destination, "unknown"), CategoryQueue, KindClient, spanID)
	if span != nil {
		span.SetString("messaging.system", stableWorkName(options.System, "unknown"))
		span.SetString("messaging.destination.name", stableWorkName(options.Destination, "unknown"))
		span.SetString("messaging.operation.type", "send")
	}
	defer func() {
		if span != nil {
			span.SetEnd(nowNs())
			if err != nil {
				span.SetStatus(StatusError, "")
			}
		}
	}()
	return operation(carrier)
}

func (r *Restlytics) RunJob(ctx context.Context, options JobOptions, operation func(context.Context) error) error {
	return r.tracer.RunJob(ctx, options, operation)
}

func (r *Restlytics) RunCommand(ctx context.Context, options CommandOptions, operation func(context.Context) (int, error)) (int, error) {
	return r.tracer.RunCommand(ctx, options, operation)
}

func (r *Restlytics) RunSchedule(ctx context.Context, options ScheduleOptions, operation func(context.Context) error) error {
	return r.tracer.RunSchedule(ctx, options, operation)
}
