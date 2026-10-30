package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/surajjagdev/ToyDB/internal/db"
)

func main() {
	dataDir := flag.String("data", "./data", "data directory")
	fsync := flag.Bool("fsync", true, "enable durability (disable only for tests)")
	ckptInterval := flag.Duration("checkpoint-interval", 5*time.Minute, "checkpoint interval")
	verbose := flag.Bool("v", false, "verbose logging")
	flag.Parse()

	database, err := db.Open(context.Background(), db.Options{
		Dir:                *dataDir,
		Fsync:              *fsync,
		CheckpointInterval: *ckptInterval,
		WALSizeThreshold:   1 << 30, // 1 GiB
		LogProgress:        *verbose,
	})
	if err != nil {
		log.Fatalf("open: %v", err)
	}

	// ... serve clients using database.Heap(1), database.TM(), etc. ...

	log.Println("shutting down")
	if err := database.Close(context.Background()); err != nil {
		log.Fatalf("close: %v", err)
	}
}
