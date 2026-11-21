package receiver

import (
	"context"
	"errors"

	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/pluginsdk"
	"go.opentelemetry.io/otel/trace"
)

// tracedPlugin is a profile's plugin with a CLIENT span around each of Dispatch, Lookup, Follow and Stop, on the core's
// side of the link alone: the plugin and its wire format are unchanged.
type tracedPlugin struct {
	pluginsdk.Plugin
	tracing *telemetry.Tracing
	profile string
}

// tracePlugin wraps plugin for profile; a nil plugin stays nil.
func tracePlugin(plugin pluginsdk.Plugin, tracing *telemetry.Tracing, profile string) pluginsdk.Plugin {
	if plugin == nil {
		return nil
	}
	return &tracedPlugin{Plugin: plugin, tracing: tracing, profile: profile}
}

func (p *tracedPlugin) call(ctx context.Context, name, run string) (context.Context, trace.Span) {
	return p.tracing.Start(ctx, "plugin "+name, trace.SpanKindClient, telemetry.AttrProfile.String(p.profile), telemetry.AttrRun.String(run))
}

func (p *tracedPlugin) Dispatch(ctx context.Context, run pluginsdk.Run) (pluginsdk.Outcome, pluginsdk.Handle, error) {
	ctx, span := p.call(ctx, "Dispatch", run.ID)
	defer span.End()
	outcome, handle, err := p.Plugin.Dispatch(ctx, run)
	failSpan(span, err)
	return outcome, handle, err
}

func (p *tracedPlugin) Lookup(ctx context.Context, runID string) (pluginsdk.Job, error) {
	ctx, span := p.call(ctx, "Lookup", runID)
	defer span.End()
	job, err := p.Plugin.Lookup(ctx, runID)
	failSpan(span, err)
	return job, err
}

func (p *tracedPlugin) Follow(ctx context.Context, handle pluginsdk.Handle, from int64, lines chan<- pluginsdk.Line) (pluginsdk.Terminal, error) {
	ctx, span := p.call(ctx, "Follow", "")
	defer span.End()
	terminal, err := p.Plugin.Follow(ctx, handle, from, lines)
	failSpan(span, err)
	return terminal, err
}

func (p *tracedPlugin) Stop(ctx context.Context, handle pluginsdk.Handle) error {
	ctx, span := p.call(ctx, "Stop", "")
	defer span.End()
	err := p.Plugin.Stop(ctx, handle)
	failSpan(span, err)
	return err
}

// failSpan sets span's status to the fixed class of a plugin call's error, and nothing of the error itself.
func failSpan(span trace.Span, err error) {
	if err != nil {
		telemetry.Fail(span, pluginClass(err))
	}
}

// pluginClass is the fixed class of a plugin call's error, from a closed set.
func pluginClass(err error) string {
	var failure *pluginsdk.Error
	var tooLarge *pluginsdk.InputTooLargeError
	switch {
	case errors.Is(err, context.Canceled):
		return telemetry.ClassCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return telemetry.ClassTimeout
	case errors.Is(err, pluginsdk.ErrLinkLost):
		return telemetry.ClassLinkLost
	case errors.Is(err, pluginsdk.ErrMessageTooLarge), errors.As(err, &tooLarge):
		return telemetry.ClassTooLarge
	case errors.As(err, &failure) && failure.Class == pluginsdk.ClassFetch:
		return telemetry.ClassFetch
	case errors.As(err, &failure):
		return telemetry.ClassPlugin
	}
	return telemetry.ClassError
}
