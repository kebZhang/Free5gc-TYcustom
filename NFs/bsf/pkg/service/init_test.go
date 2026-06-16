package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/free5gc/bsf/pkg/factory"
	"github.com/free5gc/bsf/pkg/service"
)

func TestNewApp(t *testing.T) {
	cfg := &factory.Config{
		Configuration: &factory.Configuration{
			Sbi: &factory.Sbi{
				Scheme:      "http",
				BindingIPv4: "127.0.0.1",
				Port:        8099,
			},
		},
		Logger: &factory.Logger{
			Enable:       false,
			Level:        "info",
			ReportCaller: false,
		},
	}

	ctx := context.Background()
	app, err := service.NewApp(ctx, cfg, "")
	if err != nil {
		t.Fatalf("Failed to create app: %v", err)
	}

	if app == nil {
		t.Fatal("App is nil")
	}

	if app.Config() != cfg {
		t.Error("Config mismatch")
	}

	if app.Context() == nil {
		t.Error("BSF Context is nil")
	}

	if app.Consumer() == nil {
		t.Error("Consumer is nil")
	}

	if app.Processor() == nil {
		t.Error("Processor is nil")
	}

	t.Log("NewApp successfully created BSF app with all components")
}

func TestAppStartTerminate(t *testing.T) {
	cfg := &factory.Config{
		Info: &factory.Info{
			Version:     "1.0.2",
			Description: "BSF test configuration",
		},
		Configuration: &factory.Configuration{
			Sbi: &factory.Sbi{
				Scheme:      "http",
				BindingIPv4: "127.0.0.1",
				Port:        8100,
			},
		},
		Logger: &factory.Logger{
			Enable:       false,
			Level:        "info",
			ReportCaller: false,
		},
	}
	factory.BsfConfig = cfg

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancel is called on all paths

	app, err := service.NewApp(ctx, cfg, "")
	if err != nil {
		t.Fatalf("Failed to create app: %v", err)
	}

	// Start app in goroutine
	startDone := make(chan struct{})
	go func() {
		app.Start()
		close(startDone)
	}()

	// Give server time to start
	time.Sleep(500 * time.Millisecond)

	// Trigger shutdown via Terminate (which cancels context)
	startTime := time.Now()
	app.Terminate()

	// Wait for Start to return with longer timeout
	select {
	case <-startDone:
		duration := time.Since(startTime)
		t.Logf("App stopped in %v after Terminate() call", duration)
		if duration > 15*time.Second {
			t.Errorf("App took too long to stop: %v", duration)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("App did not stop within 20 seconds")
	}
}

func TestAppInterface(t *testing.T) {
	cfg := &factory.Config{
		Configuration: &factory.Configuration{
			Sbi: &factory.Sbi{
				Scheme:      "http",
				BindingIPv4: "127.0.0.1",
				Port:        8101,
			},
		},
		Logger: &factory.Logger{
			Enable:       false,
			Level:        "info",
			ReportCaller: false,
		},
	}

	ctx := context.Background()
	app, err := service.NewApp(ctx, cfg, "")
	if err != nil {
		t.Fatalf("Failed to create app: %v", err)
	}

	// Test that BsfApp implements BsfAppInterface
	var _ service.BsfAppInterface = app

	// Test log functions don't panic
	app.SetLogEnable(false)
	app.SetLogLevel("debug")
	app.SetReportCaller(false)

	t.Log("App successfully implements all interface methods")
}
