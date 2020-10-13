package main

import (
	"flag"
	"github.com/jdextraze/go-gesclient/client"
	"github.com/jdextraze/go-gesclient/flags"
	uuid "github.com/satori/go.uuid"
	"log"
	"math/rand"
	"time"

	"fmt"
)

type ProducerEventStoreDemo struct {

	// Connection
	conn client.Connection
}

func generateRandomPayload() string {

	r := rand.Intn(4)
	evName := ""
	switch r {
	case 0:
		evName = "SalesID0"
	case 1:
		evName = "SalesID1"
	case 2:
		evName = "Other0"
	case 3:
		evName = "Other1"
	}

	amt := rand.Intn(100)
	payload := fmt.Sprintf(`{"Dept":"%s", "Amount":%d}`, evName, amt)
	return payload
}

func (e *ProducerEventStoreDemo) Spammageddon(username string, password string, server string, port int, streamName string) error {

	//////////////// Flags/options ///////////////////////
	fs := flag.NewFlagSet("esflags", flag.ExitOnError)
	flags.Init(fs)

	// Connection string to server.
	connectionString := fmt.Sprintf("%s://%s:%s@%s:%d","tcp", username, password, server, port)
	fs.Set("endpoint", connectionString)
	flag.Parse()
  //////////////////////////////////////////////////////


	///////// Create connection and connect //////////////
	var err error
	e.conn, err = flags.CreateConnection("AllCatchupSubscriber")
	if err != nil {
		log.Fatalf("Error creating connection: %v", err)
	}

	if err := e.conn.ConnectAsync().Wait(); err != nil {
		log.Fatalf("Error connecting: %v", err)
	}
	//////////////////////////////////////////////////////


	// spammageddon.
	for i:=0 ; i< 10000000 ; i++ {
		payload := generateRandomPayload()
		evt := client.NewEventData( uuid.Must(uuid.NewV4()), "SalesEventId", true, []byte(payload), nil)
		_, _ = e.conn.AppendToStreamAsync(streamName, client.ExpectedVersion_Any, []*client.EventData{evt}, nil)
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

func main() {
	fmt.Printf("So it begins...\n")

  esd := ProducerEventStoreDemo{}
  esd.Spammageddon("admin", "changeit", "127.0.0.1", 1113, "Default" )
}
