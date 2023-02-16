package mock

import (
	"testing"

	"github.com/blugnu/kafka/api"
)

type mockEvent string

func (ev mockEvent) Error() string  { return string(ev) }
func (ev mockEvent) String() string { return string(ev) }

func TestProducerApi(t *testing.T) {
	// ARRANGE
	// ACT
	api, mock := ProducerApi()

	// ASSERT
	t.Run("returns api", func(t *testing.T) {
		wanted := true
		got := api != nil
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("returns mock", func(t *testing.T) {
		wanted := true
		got := mock != nil
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}

		t.Run("with Reset called", func(t *testing.T) {
			wanted := true
			got := mock.ResetWasCalled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})
}

func TestProducer_Close(t *testing.T) {
	// ARRANGE
	api, mock := ProducerApi()

	// ACT
	api.Close()

	// ASSERT
	t.Run("calls Close", func(t *testing.T) {
		wanted := true
		got := mock.CloseWasCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func TestProducer_Create(t *testing.T) {
	// ARRANGE
	cfg := &api.ConfigMap{}
	api, mock := ProducerApi()

	// ACT
	api.Create(cfg)

	// ASSERT
	t.Run("calls Create", func(t *testing.T) {
		wanted := true
		got := mock.CreateWasCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}

		t.Run("with specified config", func(t *testing.T) {
			wanted := cfg
			got := mock.Config
			if wanted != got {
				t.Errorf("wanted %#v, got %#v", wanted, got)
			}
		})
	})
}

func TestProducer_Flush(t *testing.T) {
	// ARRANGE
	api, mock := ProducerApi()

	// ACT
	api.Flush(100)

	// ASSERT
	t.Run("calls Flush", func(t *testing.T) {
		wanted := true
		got := mock.FlushWasCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("records timeout", func(t *testing.T) {
		wanted := 100
		got := mock.FlushTimeoutMs
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func TestProducer_Produce(t *testing.T) {
	// ARRANGE
	evc := make(chan api.DeliveryEvent)
	msg := &api.Message{}
	api, mock := ProducerApi()

	// ACT
	api.Produce(msg, evc)

	// ASSERT
	t.Run("calls Produce", func(t *testing.T) {
		wanted := true
		got := mock.ProduceWasCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func TestProducer_SendDeliveryEventWhenClosingEventChannel(t *testing.T) {
	// ARRANGE
	ev := mockEvent("event")
	api, mock := ProducerApi()
	evc := api.EventChannel()
	mock.Funcs.Close = nil

	// ACT
	mock.SendDeliveryEvent(ev, true)

	// ASSERT
	t.Run("sends event to channel", func(t *testing.T) {
		rev := <-evc
		wanted := rev != nil
		got := mock.SendDeliveryEventWasCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("closes channel", func(t *testing.T) {
		isOpen := true
		defer func() {
			if r := recover(); r != nil {
				isOpen = false
			}
			wanted := true
			got := !isOpen
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		}()
		// Attempt to send to the channel - if it is closed it will panic
		select {
		case evc <- ev:
			isOpen = true
		default:
		}
	})

	t.Run("replaces Close func", func(t *testing.T) {
		wanted := true
		got := mock.Funcs.Close != nil
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func TestProducer_SendDeliveryEventWhenNotClosingEventChannel(t *testing.T) {
	// ARRANGE
	ev := mockEvent("event")
	api, mock := ProducerApi()
	evc := api.EventChannel()
	mock.Funcs.Close = nil

	// ACT
	mock.SendDeliveryEvent(ev, false)

	// ASSERT
	t.Run("sends event to channel", func(t *testing.T) {
		rev := <-evc
		wanted := rev != nil
		got := mock.SendDeliveryEventWasCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("leaves channel open", func(t *testing.T) {
		evc <- ev
		isOpen := true
		select {
		case <-evc:
		default:
			isOpen = false
		}
		wanted := true
		got := isOpen
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("does not replace Close func", func(t *testing.T) {
		wanted := true
		got := mock.Funcs.Close == nil
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}
