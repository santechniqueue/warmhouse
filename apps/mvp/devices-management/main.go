// api/main.go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v5/stdlib" // pgx as database/sql driver
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

func newKafkaWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
}

func main() {
	topic := getenv("KAFKA_TOPIC", "devices")
	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	port := getenv("HTTP_PORT", ":8080")
	writer := newKafkaWriter(brokers, topic)
	defer writer.Close()

	db, err := sql.Open("pgx", getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/smarthome"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	r := gin.Default()
	v1 := r.Group("/api/v1")

	// Create
	v1.POST("/devices", func(c *gin.Context) {
		var req Device
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		ev := event{Op: "create", Device: req}
		if err := publish(c, writer, ev); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "kafka publish failed"})
			return
		}
		// контракт: возвращаем те же поля без id
		c.JSON(http.StatusCreated, req)
	})

	// Update (PATCH)
	v1.PATCH("/devices/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil || id <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
			return
		}
		var req Device
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		req.ID = id
		ev := event{Op: "update", Device: req}
		if err := publish(c, writer, ev); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "kafka publish failed"})
			return
		}
		// контракт: 200 + тело С id
		c.JSON(http.StatusOK, req)
	})

	// Delete
	v1.DELETE("/devices/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil || id <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
			return
		}
		ev := event{Op: "delete", Device: Device{ID: id}}
		if err := publish(c, writer, ev); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "kafka publish failed"})
			return
		}
		c.Status(http.StatusNoContent)
	})

	// Get by id (читает из БД)
	v1.GET("/devices/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil || id <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
			return
		}
		var d Device
		row := db.QueryRowContext(c, `
			SELECT id, name, "type", location, COALESCE(unit,''), updated_at
			FROM devices WHERE id=$1`, id)
		var updated time.Time
		if err := row.Scan(&d.ID, &d.Name, &d.Type, &d.Location, &d.Unit, &updated); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		// контракт GET: без id в ответе
		c.JSON(http.StatusOK, gin.H{
			"name":     d.Name,
			"type":     d.Type,
			"location": d.Location,
			"unit":     d.Unit,
		})
	})

	log.Println("api started")
	_ = r.Run(port)
}

func publish(ctx context.Context, w *kafka.Writer, ev event) error {
	b, _ := json.Marshal(ev)
	msg := kafka.Message{
		Key:   []byte(ev.Op),
		Value: b,
		Time:  time.Now(),
	}
	return w.WriteMessages(ctx, msg)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
