package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hamba/avro"

	eventhub "github.com/Azure/azure-event-hubs-go"
	uuid "github.com/nu7hatch/gouuid"
)

//Member status
type Member struct {
	ID      string `avro:"ID"`
	Counter int    `avro:"Counter"`
	DtTm    string `avro:"DtTm"`
	Status  string `avro:"Status"`
}

func main() {
	connStr := "<CONNSTRING>"
	entityPath := fmt.Sprintf("EntityPath=%s", "avro")
	connStr = fmt.Sprintf("%s;%s", connStr, entityPath)
	envVarName := "AVRO"

	//Get HostName
	hname, err := os.Hostname()
	if err != nil {
		fmt.Printf("ERR:Error getting hostname.\n")
	}

	hub, err := eventhub.NewHubFromConnectionString(connStr)
	if err != nil {
		fmt.Printf("ERR:Error connecting to .\n")
	}

	schema, err := avro.Parse(`{
		"type": "record",
		"name": "member",
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

	var member Member
	for x := 0; x < 1000000000; x++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		//uuid, err := exec.Command("uuidgen").Output()
		uuid, err := uuid.NewV4()
		if err != nil {
			log.Fatal(err)
		}
		member.Counter = x
		member.DtTm = time.Now().String()
		member.ID = fmt.Sprintf("%s,%s, %s", envVarName, hname, uuid.String())
		member.Status = "New"

		bytesRepresentation, err := avro.Marshal(schema, member)
		if err != nil {
			log.Fatal(err)
		}
		//start := time.Now()
		event := eventhub.NewEvent(bytesRepresentation)
		event.PartitionKey = &member.ID
		//err = hub.Send(ctx, eventhub.NewEvent(bytesRepresentation))
		err = hub.Send(ctx, event)
		//t := time.Now()
		//elapsed := t.Sub(start)
		if err != nil {
			fmt.Println(err)
			return
		}
		//fmt.Printf("INFO:%d-%s <<>> Elapsed Time:%v\n", x, member.DtTm, elapsed)
		fmt.Println(member)
	}

	err = hub.Close(context.Background())
	if err != nil {
		fmt.Println(err)
	}
}
