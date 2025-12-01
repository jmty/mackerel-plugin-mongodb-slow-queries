package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	mp "github.com/mackerelio/go-mackerel-plugin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MongoDBSlowQueriesPlugin mackerel plugin for mongo
type MongoDBSlowQueriesPlugin struct {
	Prefix   string
	Host     string
	Port     string
	Username string
	Password string
	Database string
}

func (m MongoDBSlowQueriesPlugin) MetricKeyPrefix() string {
	if m.Prefix == "" {
		m.Prefix = "mongodb"
	}
	return m.Prefix
}

// GraphDefinition interface for mackerelplugin
func (m MongoDBSlowQueriesPlugin) GraphDefinition() map[string]mp.Graphs {
	return map[string]mp.Graphs{
		"slow_queries": {
			Label: "MongoDB Slow Queries",
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "count", Label: "Slow Queries"},
				{Name: "total_time", Label: "Total Time (ms)"},
				{Name: "average_time", Label: "Average Time (ms)"},
			},
		},
	}
}

// FetchMetrics interface for mackerelplugin
func (m MongoDBSlowQueriesPlugin) FetchMetrics() (map[string]float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build MongoDB connection URI
	var uri string
	if m.Username != "" && m.Password != "" {
		uri = fmt.Sprintf("mongodb://%s:%s@%s:%s/%s?authSource=admin",
			m.Username, m.Password, m.Host, m.Port, m.Database)
	} else {
		uri = fmt.Sprintf("mongodb://%s:%s/%s",
			m.Host, m.Port, m.Database)
	}

	// Set connection options
	clientOptions := options.Client().ApplyURI(uri)
	// Read from secondary if available, otherwise from primary
	clientOptions.SetReadPreference(readpref.SecondaryPreferred())

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "failed to disconnect from MongoDB: %v\n", err)
		}
	}()

	// Verify connection
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %v", err)
	}

	// Access system.profile collection
	collection := client.Database(m.Database).Collection("system.profile")

	// Get timestamp from 1 minute ago
	oneMinuteAgo := time.Now().Add(-1 * time.Minute)

	// Retrieve slow queries
	filter := bson.M{"ts": bson.M{"$gt": oneMinuteAgo}}
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to find documents: %v", err)
	}
	defer cursor.Close(ctx)

	// Calculate metrics
	var count int64
	var totalTimeMs float64

	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			continue
		}
		
		count++
		
		// Get execution time from millis field
		if millis, ok := result["millis"].(int32); ok {
			totalTimeMs += float64(millis)
		} else if millis, ok := result["millis"].(int64); ok {
			totalTimeMs += float64(millis)
		} else if millis, ok := result["millis"].(float64); ok {
			totalTimeMs += millis
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %v", err)
	}

	// Calculate average time
	var averageTimeMs float64
	if count > 0 {
		averageTimeMs = totalTimeMs / float64(count)
	}

	return map[string]float64{
		"count":        float64(count),
		"total_time":   totalTimeMs,
		"average_time": averageTimeMs,
	}, nil
}

// Do the plugin
func main() {
	optPrefix := flag.String("metric-key-prefix", "mongodb", "Metric key prefix")
	optHost := flag.String("host", "localhost", "Hostname")
	optPort := flag.String("port", "27017", "Port")
	optUser := flag.String("username", "", "Username")
	optPass := flag.String("password", os.Getenv("MONGODB_PASSWORD"), "Password")
	optDatabase := flag.String("database", "", "Database name")
	flag.Parse()

	if *optDatabase == "" {
		fmt.Fprintln(os.Stderr, "Database name is required")
		flag.Usage()
		os.Exit(1)
	}

	var mongodb MongoDBSlowQueriesPlugin
	mongodb.Prefix = *optPrefix
	mongodb.Host = *optHost
	mongodb.Port = *optPort
	mongodb.Username = *optUser
	mongodb.Password = *optPass
	mongodb.Database = *optDatabase

	helper := mp.NewMackerelPlugin(mongodb)
	helper.Run()
}