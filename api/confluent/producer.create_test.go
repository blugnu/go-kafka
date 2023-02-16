package confluent

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/blugnu/kafka/api"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestProducer_Create(t *testing.T) {
	// ARRANGE
	//ctx := context.Background()

	sut := &Producer{
		//Log:         &logger.Base{Context: ctx, Adapter: &logger.LogrusAdapter{Logger: logrus.New()}},
	}

	t.Run("calls NewProducer()", func(t *testing.T) {
		// ARRANGE
		cm := &api.ConfigMap{"key": "value"}

		ccm := &confluent.ConfigMap{}
		cfgerr := errors.New("configuration test")

		ofn := newProducer
		defer func() { newProducer = ofn }()
		newProducer = func(cfg *confluent.ConfigMap) (*confluent.Producer, error) {
			ccm = cfg
			return nil, cfgerr
		}

		// ACT
		err := sut.Create(cm)

		// ASSERT
		t.Run("with confluent config", func(t *testing.T) {
			wanted := &confluent.ConfigMap{
				"key": "value",
			}
			got := ccm
			if !reflect.DeepEqual(wanted, got) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})

		t.Run("returns error", func(t *testing.T) {
			wanted := cfgerr
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("when newProducer() fails", func(t *testing.T) {
		// ARRANGE
		ncerr := errors.New("newProducer error")
		ofn := newProducer
		defer func() { newProducer = ofn }()
		newProducer = func(*confluent.ConfigMap) (*confluent.Producer, error) { return nil, ncerr }

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

	t.Run("when newProducer() succeeds", func(t *testing.T) {
		// ARRANGE
		{
			ofn := newProducer
			defer func() { newProducer = ofn }()
			newProducer = func(*confluent.ConfigMap) (*confluent.Producer, error) { return &confluent.Producer{}, nil }
		}

		icerr := errors.New("initConsumer error")
		{
			ofn := initConsumer
			defer func() { initConsumer = ofn }()
			initConsumer = func(*Consumer) error { return icerr }
		}

		// ACT
		err := sut.Create(&api.ConfigMap{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
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
