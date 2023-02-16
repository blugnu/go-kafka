package confluent

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/blugnu/kafka/api"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestConsumer_Create(t *testing.T) {
	// ARRANGE
	//ctx := context.Background()

	sut := &Consumer{
		//Log:         &logger.Base{Context: ctx, Adapter: &logger.LogrusAdapter{Logger: logrus.New()}},
	}

	t.Run("calls NewConsumer()", func(t *testing.T) {
		// ARRANGE
		cm := &api.ConfigMap{"key": "value"}

		ccm := &confluent.ConfigMap{}

		ofn := newConsumer
		defer func() { newConsumer = ofn }()
		newConsumer = func(cfg *confluent.ConfigMap) (*confluent.Consumer, error) {
			ccm = cfg
			return nil, errors.New("configuration test")
		}

		// ACT
		sut.Create(cm)

		// ASSERT
		t.Run("enables logs channel", func(t *testing.T) {
			// ARRANGE
			key := "go.logs.channel.enable"

			// ACT
			got, _ := ccm.Get(key, nil)

			// ASSERT
			wanted := true
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("when newConsumer() fails", func(t *testing.T) {
		// ARRANGE
		ncerr := errors.New("newConsumer error")
		ofn := newConsumer
		defer func() { newConsumer = ofn }()
		newConsumer = func(*confluent.ConfigMap) (*confluent.Consumer, error) { return nil, ncerr }

		// ACT
		err := sut.Create(&api.ConfigMap{})

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := ncerr
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("when newConsumer() succeeds", func(t *testing.T) {
		// ARRANGE
		{
			ofn := newConsumer
			defer func() { newConsumer = ofn }()
			newConsumer = func(*confluent.ConfigMap) (*confluent.Consumer, error) { return &confluent.Consumer{}, nil }
		}

		icerr := errors.New("initConsumer error")
		{
			ofn := initConsumer
			defer func() { initConsumer = ofn }()
			initConsumer = func(*Consumer) error { return icerr }
		}

		// ACT
		err := sut.Create(&api.ConfigMap{})

		// ASSERT
		t.Run("returns any error from initConsumer", func(t *testing.T) {
			wanted := icerr
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})

		t.Run("assigns funcs", func(t *testing.T) {
			funcs := reflect.ValueOf(*sut.funcs)
			funcsType := funcs.Type()

			for i := 0; i < funcsType.NumField(); i++ {
				t.Run(fmt.Sprintf("c.funcs.%s", funcsType.Field(i).Name), func(t *testing.T) {
					wanted := true
					got := !reflect.ValueOf(funcs.Field(i).Interface()).IsNil()
					if wanted != got {
						t.Errorf("wanted %v, got %v", wanted, got)
					}
				})
			}
		})
	})
}
