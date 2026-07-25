package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ai-crypto-onramp/exchange-connector/internal/adapters/binance"
	"github.com/ai-crypto-onramp/exchange-connector/internal/adapters/kraken"
	"github.com/ai-crypto-onramp/exchange-connector/internal/adapters/otc"
	"github.com/ai-crypto-onramp/exchange-connector/internal/audit"
	"github.com/ai-crypto-onramp/exchange-connector/internal/events"
	"github.com/ai-crypto-onramp/exchange-connector/internal/otel"
	"github.com/ai-crypto-onramp/exchange-connector/internal/server"
	"github.com/ai-crypto-onramp/exchange-connector/internal/store"
	"github.com/ai-crypto-onramp/exchange-connector/internal/store/postgres"
	"github.com/ai-crypto-onramp/exchange-connector/internal/venue"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
	shutdown, err := otel.Init("exchange-connectors")
	if err != nil {
		log.Fatalf("otel init: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	devMode := os.Getenv("DEV_MODE") == "1"
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	venueName := os.Getenv("VENUE_FAMILY")
	if venueName == "" {
		venueName = "dummy"
	}
	pairs := []string{"BTCUSDT", "ETHUSDT"}
	if p := os.Getenv("PAIRS"); p != "" {
		pairs = splitCSV(p)
	}

	conn := selectConnector(venueName, devMode)
	if devMode {
		log.Printf("DEV_MODE=1: reading exchange creds from env (NOT FOR PRODUCTION); venue=%s", venueName)
	}
	sink := newAuditSink(venueName, devMode)

	// When EVENT_BUS_URL is set (kafka://host:9092), construct a Kafka
	// publisher + events.Bus for fill/balance events and close it on
	// shutdown. When unset, the events bus is nil (events disabled).
	var eventPub *events.KafkaPublisher
	var bus *events.Bus
	if busURL := os.Getenv("EVENT_BUS_URL"); busURL != "" {
		p, err := events.NewKafkaPublisherFromURL(busURL)
		if err != nil {
			log.Printf("events: kafka publisher init failed: %v", err)
		} else {
			eventPub = p
			bus = events.NewBus(p, "recon")
		}
	}

	svc, err := server.NewService(conn, sink, server.Config{VenueName: venueName, Pairs: pairs, Store: newStore(devMode)}, bus)
	if err != nil {
		log.Fatalf("server: %v", err)
	}

	srv := &http.Server{
		Addr:    addr,
		Handler: otelhttp.NewHandler(svc.Routes(), "exchange-connectors"),
	}

	go func() {
		log.Printf("exchange-connectors listening on %s (venue=%s)", addr, venueName)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	if eventPub != nil {
		_ = eventPub.Close()
	}
}

func selectConnector(name string, devMode bool) venue.VenueConnector {
	switch name {
	case "binance":
		key, secret := loadExchangeCreds("binance", devMode)
		return binance.NewConnector(key, secret, nil)
	case "kraken":
		key, secret := loadExchangeCreds("kraken", devMode)
		return kraken.NewConnector(key, secret, nil)
	case "otc":
		key, _ := loadExchangeCreds("otc", devMode)
		return otc.NewConnector(key, nil)
	default:
		if !devMode {
			log.Fatalf("VENUE_FAMILY=%q not a real venue and DEV_MODE!=1; refusing to start with dummy connector in production mode", name)
		}
		return venue.NewDummyVenueConnector()
	}
}

// loadExchangeCreds loads exchange API credentials. In production it must
// come from a secrets manager (e.g. Vault via secrets.Manager); the env-var
// path is only permitted when DEV_MODE=1.
func loadExchangeCreds(venue string, devMode bool) (apiKey, apiSecret string) {
	if devMode {
		apiKey = os.Getenv(strings.ToUpper(venue) + "_API_KEY")
		apiSecret = os.Getenv(strings.ToUpper(venue) + "_API_SECRET")
		return
	}
	// Production: secrets manager integration is not yet wired; require the
	// URL and refuse to start without it.
	if os.Getenv("SECRETS_MANAGER_URL") == "" {
		log.Fatalf("SECRETS_MANAGER_URL required in production mode; secrets.Manager integration not yet wired — set DEV_MODE=1 for local dev")
	}
	// Placeholder until secrets.Manager is wired at the composition root.
	// Fatal above guards production; the env path stays reachable only in dev.
	log.Fatalf("SECRETS_MANAGER_URL set but secrets.Manager wiring not yet implemented — set DEV_MODE=1 for local dev")
	return
}

func splitCSV(s string) []string {
	var out []string
	cur := ""
	for _, c := range s {
		if c == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
		} else {
			cur += string(c)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func newStore(devMode bool) store.Store {
	dsn := os.Getenv("DB_URL")
	if dsn != "" {
		db, err := postgres.Open(context.Background(), dsn)
		if err != nil {
			log.Fatalf("postgres: open: %v", err)
		}
		return db
	}
	if devMode {
		log.Printf("WARNING: DEV_MODE=1 with no DB_URL — using in-memory store; all state is lost on restart")
		return store.New()
	}
	log.Fatalf("DB_URL required in production mode — set DEV_MODE=1 to allow in-memory store for development")
	return store.New()
}

func newAuditSink(venue string, devMode bool) audit.Sink {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		if devMode {
			log.Printf("warn: KAFKA_BROKERS unset and DEV_MODE=1; audit events recorded in-memory only")
			return audit.NewInMemorySink()
		}
		log.Fatalf("KAFKA_BROKERS unset and DEV_MODE not set; cannot start audit producer")
	}
	ks, err := audit.NewKafkaSink(splitCSV(brokers), venue)
	if err != nil {
		if devMode {
			log.Printf("warn: audit kafka init failed (DEV_MODE): %v; falling back to in-memory", err)
			return audit.NewInMemorySink()
		}
		log.Fatalf("audit kafka init: %v", err)
	}
	return ks
}