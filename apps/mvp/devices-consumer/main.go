package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"
)

type Device struct {
	ID       int64  `json:"id,omitempty"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Location string `json:"location"`
	Unit     string `json:"unit"`
}

type event struct {
	Op     string `json:"op"`
	Device Device `json:"device"`
}

func main() {
	db, err := sql.Open("pgx", getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/smarthome"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := getenv("KAFKA_TOPIC", "devices")
	group := getenv("KAFKA_GROUP", "devices-consumer")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     group,
		StartOffset: kafka.FirstOffset,
		MaxWait:     500 * time.Millisecond,
	})
	defer r.Close()

	log.Println("consumer started; topic:", topic)
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Println("kafka read error:", err)
			continue
		}
		var ev event
		if err := json.Unmarshal(m.Value, &ev); err != nil {
			log.Println("bad event:", err, string(m.Value))
			continue
		}
		if err := apply(db, ev); err != nil {
			log.Println("apply error:", err, "event:", string(m.Value))
			continue
		}
	}
}

func apply(db *sql.DB, ev event) error {
	switch ev.Op {
	case "create":
		_, err := db.Exec(`
			INSERT INTO devices (name, "type", location, unit)
			VALUES ($1,$2,$3,$4)`,
			ev.Device.Name, ev.Device.Type, ev.Device.Location, ev.Device.Unit)
		return err
	case "update":
		_, err := db.Exec(`
			UPDATE devices
			SET name=$2, "type"=$3, location=$4, unit=$5, updated_at=now()
			WHERE id=$1`,
			ev.Device.ID, ev.Device.Name, ev.Device.Type, ev.Device.Location, ev.Device.Unit)
		return err
	case "delete":
		_, err := db.Exec(`DELETE FROM devices WHERE id=$1`, ev.Device.ID)
		return err
	default:
		log.Println("unknown op:", ev.Op)
		return nil
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
