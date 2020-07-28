package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go"
	"github.com/hamba/avro"
)

var pluginSvc = ""
var entityPath = ""
var catcherSvc = ""

//Member status
type Member struct {
	ID      string `json:"id,omitempty"`
	Counter int    `json:"counter,omitempty"`
	DtTm    string `json:"dttm,omitempty"`
	Status  string `json:"status,omitempty"`
}

//Partition helps with reading through the partitions
func Partition(conn string, partitionid string) {
	hub, err := eventhub.NewHubFromConnectionString(conn)
	if err != nil {
		LogIt("ERR:Could not connect to Event Hub", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Hour)
	defer cancel()
	//ctx := context.Background()

	_, err = hub.Receive(ctx, partitionid, handler, eventhub.ReceiveWithLatestOffset())
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("INFO:I am listening to partition %s...\n", partitionid)
}

func main() {

	connStr := "<CONNSTRING>"
	entityPath := fmt.Sprintf("EntityPath=%s", "avro")
	connStr = fmt.Sprintf("%s;%s", connStr, entityPath)

	hub, err := eventhub.NewHubFromConnectionString(connStr)
	if err != nil {
		LogIt("ERR:Could not connect to Event Hub", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Hour)
	defer cancel()

	info, err := hub.GetRuntimeInformation(ctx)
	if err != nil {
		log.Fatalf("failed to get runtime info: %s\n", err)
	}
	log.Printf("got partition IDs: %s\n", info.PartitionIDs)

	for _, partitionID := range info.PartitionIDs {
		go Partition(connStr, partitionID)
	}

	//Wait for a signal to quit
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan
	hub.Close(context.Background())
}

//LogIt the Logger
func LogIt(pipeline string, err error) {
	fmt.Printf("%s,%s\n", pipeline, err)
}

//Handle EventHub Messages
func handler(c context.Context, event *eventhub.Event) error {

	var member Member
	schema, err := avro.Parse(`{
		"type": "record",
		"name": "meember",
		"namespace": "org.jaypaddy.avro",
		"fields" : [
			{"name": "ID", "type": "string"},
			{"name": "Counter", "type": "int"},
			{"name": "DtTm", "type": "string"},
			{"name": "Status", "type": "string"}
		]
	}`)
	if err != nil {
		log.Fatal(err)
	}

	err = avro.Unmarshal(schema, event.Data, &member)
	if err != nil {
		log.Fatal(err)
	}
	//fmt.Printf("RCVD: %d \n", member.Counter)
	fmt.Println(member)
	return nil
}
