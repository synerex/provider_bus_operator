package main

import (
	"context"
	"flag"
	"fmt"

	json "github.com/synerex/proto_json"
	rcm "github.com/synerex/proto_recommend"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"

	"log"
	"sync"
	"time"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	mu              sync.Mutex
	version         = "0.0.0"
	role            = "BusOperator"
	sxServerAddress string
)

func init() {
	flag.Parse()
}

func supplyRecommendCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	recommend := &rcm.Recommend{}
	log.Printf("Received Recommend Supply from %d %+v", sp.SenderId, recommend)
}

func subscribeRecommendSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyRecommendCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
}

func supplyJsonRecordCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	jsonRecord := &json.JsonRecord{}
	log.Printf("Received JsonRecord Supply from %d %+v", sp.SenderId, jsonRecord.Json)
}

func subscribeJsonRecordSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyJsonRecordCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
}

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock()
	if client.SXClient != nil {
		client.SXClient = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.SXClient == nil {
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.SXClient = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server [%s]\n", sxServerAddress)
	}
	mu.Unlock()
}

func main() {
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	log.Printf("%s(%s) built %s sha1 %s", role, sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	channelTypes := []uint32{pbase.GRIDEYE_SVC}

	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, role, channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		sxServerAddress = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", sxServerAddress)

	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(sxServerAddress)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	} else {
		log.Print("Connecting SynerexServer")
	}

	rcmClient := sxutil.NewSXServiceClient(client, pbase.ALT_PT_SVC, fmt.Sprintf("{Client:%s}", role))
	envClient := sxutil.NewSXServiceClient(client, pbase.JSON_DATA_SVC, fmt.Sprintf("{Client:%s}", role))

	wg.Add(1)
	log.Print("Subscribe Supply")
	go subscribeRecommendSupply(rcmClient)
	go subscribeJsonRecordSupply(envClient)
	wg.Wait()
}
