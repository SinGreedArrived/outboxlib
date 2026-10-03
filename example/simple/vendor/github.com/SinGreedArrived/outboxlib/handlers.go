package outboxlib

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type Handler func(ctx context.Context, v json.RawMessage) (json.RawMessage, error)


type handlersRegistrator struct {
	mu       sync.RWMutex
	handlers map[HandlerName]Handler
}

var def = NewRegistrator()

func Register[T,R any](
	name HandlerName, 
	handler func(ctx context.Context, v T)(R,error),
) error {
	return def.Register(name, handler)
}

func Call(ctx context.Context, name HandlerName, req json.RawMessage) (json.RawMessage, error) {
	return def.Call(ctx, name, req)
}

func NewRegistrator() *handlersRegistrator {
	return &handlersRegistrator{
		handlers: make(map[HandlerName]Handler),
	}
}

func (h *handlersRegistrator) register(name HandlerName, fn Handler) error {
	if _, ok := h.handlers[name]; ok {
		return fmt.Errorf("handler name busy")
	}

	h.mu.Lock()
	h.handlers[name] = fn
	h.mu.Unlock()

	return nil
}

func (h *handlersRegistrator) Register[T,R any](
	name HandlerName, 
	handler func(ctx context.Context, v T)(R,error),
) error {
	return h.register(name, func(ctx context.Context, v json.RawMessage) (json.RawMessage, error){
		var (
			arg T
		)

		if err := json.Unmarshal(v, &arg); err != nil {
			return nil, fmt.Errorf("json.Unmarshal: %w", err)
		}

		response, err := handler(ctx, arg)
		if err != nil {
			return nil, fmt.Errorf("handler: %w", err)
		}

		data, err := json.Marshal(response)
		if err != nil {
			return nil, fmt.Errorf("Marshal: %w", err)
		}

		return data, nil
	})
}

func (h *handlersRegistrator) Call(
	ctx context.Context,
	name HandlerName,
	req json.RawMessage,
) (json.RawMessage, error) {
	h.mu.RLock()
	handler, ok := h.handlers[name]
	h.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("handler %q not found", name)
	}

	respRaw, err := handler(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("fn: %w", err)
	}

	return respRaw, nil
}
