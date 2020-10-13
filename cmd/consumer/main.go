package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jdextraze/go-gesclient/client"
	"github.com/jdextraze/go-gesclient/flags"
	"log"
	"os"
	"os/signal"
)

type SalesEvent struct {
	Dept string
	Amount int
}

type ConsumerEventStoreDemo struct {

	// Connection
	conn client.Connection

	// Subscription
	subscription client.CatchUpSubscription

	// total sales.
	subTotalSales int

	// total total.
	total int
}

func (e *ConsumerEventStoreDemo) ConnectAndSubscribe(username string, password string, server string, port int, streamName string) error {

	//////////////// Flags/options ///////////////////////
	// setup some default flags
	fs := flag.NewFlagSet("esflags", flag.ExitOnError)
	flags.Init(fs)

	// Connection string to server.
	connectionString := fmt.Sprintf("%s://%s:%s@%s:%d","tcp", username, password, server, port)
	fs.Set("endpoint", connectionString)
	flag.Parse()
	//////////////////////////////////////////////////////


	///////// Create connection and connect /////////////
	var err error
	e.conn, err = flags.CreateConnection("myconnection")
	if err != nil {
		log.Fatalf("Error creating connection: %v", err)
	}

	if err := e.conn.ConnectAsync().Wait(); err != nil {
		log.Fatalf("Error connecting: %v", err)
	}
	//////////////////////////////////////////////////////

	settings := client.NewCatchUpSubscriptionSettings(client.CatchUpDefaultMaxPushQueueSize,
		client.CatchUpDefaultReadBatchSize, flags.Verbose(), true)

	// Start processing from THIS event onwards!!!
	startFromEvent := 0
	e.subscription, err = e.conn.SubscribeToStreamFrom(streamName, &startFromEvent, settings, e.processEvent, nil, nil, nil)
	if err != nil {
		log.Fatalf("Unable to subscribe... BOOM %s\n", err.Error())
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch

	e.subscription.Stop()

	return nil
}

func (e *ConsumerEventStoreDemo) processEvent(_ client.CatchUpSubscription, event *client.ResolvedEvent) error {
	data := event.OriginalEvent().Data()
	var ev SalesEvent
	json.Unmarshal(data, &ev)
	//log.Printf("dept %s : amount %d\n", ev.Dept, ev.Amount)

	if ev.Dept == "SalesID0" || ev.Dept == "SalesID1" {
		e.processSalesEvent(ev)
	}

	return nil
}

func (e *ConsumerEventStoreDemo) processSalesEvent(se SalesEvent) error {
	e.subTotalSales += se.Amount

	fmt.Printf("total sales $%d\n", e.subTotalSales)
	return nil
}

func main() {
	fmt.Printf("So it begins...\n")

  esd := ConsumerEventStoreDemo{}
  esd.ConnectAndSubscribe("admin", "changeit", "localhost",1113, "Default")

}
