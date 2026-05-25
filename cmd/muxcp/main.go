package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/moray95/muxcp/internal/gateway"
)

func main() {
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	resolvedPath, err := gateway.FindConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	slog.Info("using config", "path", resolvedPath)

	cfg, err := gateway.LoadConfig(resolvedPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	if cfg.Transport == gateway.TransportStdio {
		slog.Info("loaded config", "servers", len(cfg.Servers), "transport", cfg.Transport)
	} else {
		slog.Info("loaded config", "servers", len(cfg.Servers), "listen", cfg.Listen, "transport", cfg.Transport)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if cfg.Transport == gateway.TransportStdio {
		go watchParent(ctx, cancel, os.Getppid, func() {
			time.AfterFunc(5*time.Second, func() {
				slog.Error("forcing shutdown after parent process exited")
				os.Exit(0)
			})
		})
	}

	gw := gateway.NewGateway(cfg)

	if err := gw.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		gw.Shutdown()
		cancel()
		slog.Error("gateway error", "error", err)
		os.Exit(1) //nolint:gocritic // intentional exit on startup failure
	}

	slog.Info("shutting down...")
	gw.Shutdown()
}

func watchParent(ctx context.Context, cancel context.CancelFunc, getppid func() int, onOrphan func()) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if getppid() == 1 {
				slog.Warn("parent process exited; shutting down stdio gateway")
				cancel()
				onOrphan()
				return
			}
		}
	}
}
