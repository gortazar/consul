package submatview

import "github.com/hashicorp/consul/proto/pbsubscribe"

type eventHandler func(events *pbsubscribe.Event) (eventHandler, error)

func (v *Materializer) initialHandler(index uint64) eventHandler {
	if index == 0 {
		return newSnapshotHandler(v)
	}
	return v.resumeStreamHandler
}

type snapshotHandler struct {
	material *Materializer
	events   []*pbsubscribe.Event
}

func newSnapshotHandler(m *Materializer) eventHandler {
	return (&snapshotHandler{material: m}).handle
}

func (h *snapshotHandler) handle(event *pbsubscribe.Event) (eventHandler, error) {
	if !event.GetEndOfSnapshot() {
		h.events = append(h.events, eventsFromEvent(event)...)
		return h.handle, nil
	}

	err := h.material.updateView(h.events, event.Index)
	return h.material.eventStreamHandler, err
}

func (v *Materializer) eventStreamHandler(event *pbsubscribe.Event) (eventHandler, error) {
	err := v.updateView(eventsFromEvent(event), event.Index)
	return v.eventStreamHandler, err
}

func eventsFromEvent(event *pbsubscribe.Event) []*pbsubscribe.Event {
	if batch := event.GetEventBatch(); batch != nil {
		return batch.Events
	}
	return []*pbsubscribe.Event{event}
}

func (v *Materializer) resumeStreamHandler(event *pbsubscribe.Event) (eventHandler, error) {
	if !event.GetNewSnapshotToFollow() {
		return v.eventStreamHandler(event)
	}

	v.reset()
	return newSnapshotHandler(v), nil
}
