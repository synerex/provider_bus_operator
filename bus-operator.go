package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"

	rcm "github.com/synerex/proto_recommend"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

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
	typeProp        = "type"
	臨時便             = "臨時便"
	proposedSpIds   []uint64
)

func init() {
	flag.Parse()
}

func supplyRecommendDemandCallback(clt *sxutil.SXServiceClient, dm *api.Demand) {
	recommend := &rcm.Recommend{}
	if dm.Cdata != nil {
		err := proto.Unmarshal(dm.Cdata.Entity, recommend)
		if err == nil {
			log.Printf("Received Recommend Demand: Demand %+v, Recommend %+v", dm, recommend)
		}
	} else {
		ta := gjson.Get(dm.ArgJson, typeProp)
		if ta.Type == gjson.String && ta.Str == 臨時便 {
			log.Printf("Received JsonRecord 臨時便 Demand: Demand %+v, JSON: %s", dm, dm.ArgJson)

			var result map[string]interface{}
			err := json.Unmarshal([]byte(dm.ArgJson), &result)
			if err != nil {
				fmt.Println("Error parsing JSON:", err)
				return
			}

			index, ok := result["index"].(float64)
			if !ok {
				fmt.Println("Error: index is not a number, defaulting to 0")
				index = 0
			} else {
				fmt.Printf("ID: %d\n", int(index))
			}

			demandDepartureTime, ok := result["demand_departure_time"].(float64)
			if !ok {
				fmt.Println("Error: demand_departure_time is not a number, defaulting to 0")
				demandDepartureTime = 0
			} else {
				fmt.Printf("Demand Departure Time: %d\n", int(demandDepartureTime))
			}

			url := fmt.Sprintf(`http://host.docker.internal:5000/api/v0/bus_diagram_adjust?index=%d&demand_departure_time=%d`, index, demandDepartureTime)
			resp, err := http.Get(url)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer resp.Body.Close()

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			fmt.Println("Response:", string(body))

			spo := sxutil.SupplyOpts{
				Name: role,
				JSON: fmt.Sprintf(`{ "index": %d , "demand_departure_time": %d, "type": "臨時便", "vehicle": "マイクロバス", "date": "ASAP", "from": "岩倉駅", "to": "江南駅", "stops": "none", "way": "round-trip", "repetition": 4 }`, index, demandDepartureTime),
			}
			spid := clt.ProposeSupply(&spo)
			proposedSpIds = append(proposedSpIds, spid)
			log.Printf("#2 ProposeSupply OK! spo: %#v, spid: %d\n", spo, spid)
		}

		flag := false
		for _, pdid := range proposedSpIds {
			if pdid == dm.TargetId {
				flag = true
				log.Printf("Received JsonRecord Demand for me: Demand %+v, JSON: %s", dm, dm.ArgJson)
				err := clt.Confirm(sxutil.IDType(dm.Id), sxutil.IDType(dm.Id))
				if err != nil {
					log.Printf("#8 Confirm Send Fail! %v\n", err)
				} else {
					log.Printf("#8 Confirmed! %+v\n", dm)
				}
			}
		}
		if !flag {
			log.Printf("Received JsonRecord Demand for others: Demand %+v, JSON: %s", dm, dm.ArgJson)
		}
	}
}

func supplyRecommendSupplyCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	recommend := &rcm.Recommend{}
	if sp.Cdata != nil {
		err := proto.Unmarshal(sp.Cdata.Entity, recommend)
		if err == nil {
			log.Printf("Received Recommend Supply: Supply %+v, Recommend %+v", sp, recommend)
		}
	} else {
		log.Printf("Received JsonRecord Supply: Supply %+v, JSON: %s", sp, sp.ArgJson)
	}
}

func subscribeRecommendSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyRecommendSupplyCallback)
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

	channelTypes := []uint32{pbase.ALT_PT_SVC} //, pbase.JSON_DATA_SVC}

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
	// envClient := sxutil.NewSXServiceClient(client, pbase.JSON_DATA_SVC, fmt.Sprintf("{Client:%s}", role))

	wg.Add(1)
	log.Print("Subscribe Supply")
	go subscribeRecommendSupply(rcmClient)
	sxutil.SimpleSubscribeDemand(rcmClient, supplyRecommendDemandCallback)
	// go subscribeJsonRecordSupply(envClient)

	// タイマーを開始する
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	// 現在時刻を取得し、次の実行時刻まで待機する
	start := time.Now()
	adjust := start.Truncate(15 * time.Second).Add(15 * time.Second)
	time.Sleep(adjust.Sub(start))

	for {
		select {
		case t := <-ticker.C:
			// ここに実行したい処理を書く
			fmt.Println("実行時刻:", t.Format("15:04:05"))
			smo := sxutil.SupplyOpts{
				Name: role,
				JSON: fmt.Sprintf(`{ "%s": null }`, role), // ここにバス運行状況を入れる
			}
			_, nerr := rcmClient.NotifySupply(&smo)
			if nerr != nil {
				log.Printf("Send Fail! %v\n", nerr)
			} else {
				//							log.Printf("Sent OK! %#v\n", ge)
			}
		}
	}

	wg.Wait()
}
