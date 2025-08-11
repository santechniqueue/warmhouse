package main

import (
	"math/rand"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type TemperatureResponse struct {
	Location string  `json:"location"`
	SensorID string  `json:"sensorId"`
	Value    float64 `json:"value"`
	Unit     string  `json:"unit"`
	At       string  `json:"at"`
}

func main() {
	router := gin.Default()
	api := router.Group("/api/v1")

	api.GET("temperature", func(c *gin.Context) {
		location := c.Query("location")
		sensorID := c.Query("sensorID")

		if location == "" {
			switch sensorID {
			case "1":
				location = "Living Room"
			case "2":
				location = "Bedroom"
			case "3":
				location = "Kitchen"
			default:
				location = "Unknown"
			}
		}

		if sensorID == "" {
			switch location {
			case "Living Room":
				sensorID = "1"
			case "Bedroom":
				sensorID = "2"
			case "Kitchen":
				sensorID = "3"
			default:
				sensorID = "0"
			}
		}

		// Create new RNG with its own seed
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		val := 16.0 + rng.Float64()*(30.0-16.0)
		val = float64(int(val*10+0.5)) / 10.0

		resp := TemperatureResponse{
			Location: location,
			SensorID: sensorID,
			Value:    val,
			Unit:     "C",
			At:       time.Now().UTC().Format(time.RFC3339),
		}

		c.JSON(http.StatusOK, resp)
	})

	router.Run(":8081")
}
