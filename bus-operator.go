package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"

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
	nodesrv              = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local                = flag.String("local", "", "Local Synerex Server")
	mu                   sync.Mutex
	version              = "0.0.0"
	role                 = "BusOperator"
	sxServerAddress      string
	rcmClient            *sxutil.SXServiceClient
	typeProp             = "type"
	臨時便                  = "臨時便"
	ダイヤ調整                = "ダイヤ調整"
	proposedSpIds        []uint64
	wantBusDiagramAdjust = false
	diagramIndex         = 0
	demandDepartureTime  = 0
)

func init() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
}

type BusDiagramAdjust struct {
	Want                bool   `json:"want"`
	Area                string `json:"area"`
	Index               int    `json:"index"`
	DemandDepartureTime int    `json:"demand_departure_time"`
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
			log.Printf("Received JsonRecord %s Demand: Demand %+v, JSON: %s", 臨時便, dm, dm.ArgJson)

			// 90% の確率で臨時便を調達できると判断するとして NotifyDemand を実行
			randomNumber := rand.Intn(10)
			if randomNumber < 9 {

				spo := sxutil.SupplyOpts{
					Name: role,
					JSON: fmt.Sprintf(`{ "type": "%s", "vehicle": "マイクロバス", "date": "ASAP", "from": "岩倉駅", "to": "江南駅", "stops": "none", "way": "round-trip", "repetition": 4 }`, 臨時便),
				}
				spid := rcmClient.ProposeSupply(&spo)
				proposedSpIds = append(proposedSpIds, spid)
				log.Printf("#2 ProposeSupply OK! spo: %#v, spid: %d\n", spo, spid)
			} else {
				log.Printf("臨時便調達不能…\n")
			}

		}

		if ta.Type == gjson.String && ta.Str == ダイヤ調整 {
			log.Printf("Received JsonRecord %s Demand: Demand %+v, JSON: %s", ダイヤ調整, dm, dm.ArgJson)

			var result map[string]interface{}
			err := json.Unmarshal([]byte(dm.ArgJson), &result)
			if err != nil {
				fmt.Println("Error parsing JSON:", err)
				return
			}

			diagramIndexFloat, ok := result["index"].(float64)
			diagramIndex = int(diagramIndexFloat)
			log.Printf("%#v,%#v", diagramIndexFloat, diagramIndex)
			if !ok || err != nil {
				fmt.Println("Error: index is not a number, defaulting to 0")
				diagramIndex = 0
			} else {
				fmt.Printf("Index: %d\n", int(diagramIndex))
			}

			demandDepartureTimeFloat, ok := result["demand_departure_time"].(float64)
			demandDepartureTime = int(demandDepartureTimeFloat)
			if !ok {
				fmt.Println("Error: demand_departure_time is not a number, defaulting to 0")
				demandDepartureTime = 0
			} else {
				fmt.Printf("Demand Departure Time: %d\n", int(demandDepartureTime))
			}

			wantBusDiagramAdjust = true
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

func postBusDiagramAdjustHandler(w http.ResponseWriter, r *http.Request) {
	availableStr := r.URL.Query().Get("available")
	indexStr := r.URL.Query().Get("index")
	index, err := strconv.Atoi(indexStr)
	demandDepartureTimeStr := r.URL.Query().Get("demand_departure_time")
	demandDepartureTime, err2 := strconv.Atoi(demandDepartureTimeStr)

	if err == nil && err2 == nil && availableStr == "True" && index > 0 {
		spo := sxutil.SupplyOpts{
			Name: role,
			JSON: fmt.Sprintf(`{ "index": %d , "demand_departure_time": %d, "type": "%s" }`, index, demandDepartureTime, ダイヤ調整),
		}
		spid := rcmClient.ProposeSupply(&spo)
		proposedSpIds = append(proposedSpIds, spid)
		log.Printf("#2 ProposeSupply OK! spo: %#v, spid: %d\n", spo, spid)
	}

	status := BusDiagramAdjust{Want: false, Area: "B", Index: index, DemandDepartureTime: demandDepartureTime}

	log.Printf("Called /api/v0/post_bus_diagram_adjust (available: %+v) -> Response: %+v\n", availableStr, status)
	response, err := json.Marshal(status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

func wantBusDiagramAdjustHandler(w http.ResponseWriter, r *http.Request) {
	status := BusDiagramAdjust{Want: false}
	if wantBusDiagramAdjust {
		status.Want = true
		status.Area = "B"
		status.Index = diagramIndex
		status.DemandDepartureTime = demandDepartureTime
		wantBusDiagramAdjust = false
	}

	log.Printf("Called /api/v0/want_bus_diagram_adjust -> Response: %+v\n", status)
	response, err := json.Marshal(status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
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

	rcmClient = sxutil.NewSXServiceClient(client, pbase.ALT_PT_SVC, fmt.Sprintf("{Client:%s}", role))
	// envClient := sxutil.NewSXServiceClient(client, pbase.JSON_DATA_SVC, fmt.Sprintf("{Client:%s}", role))

	wg.Add(1)
	log.Print("Subscribe Supply")
	go subscribeRecommendSupply(rcmClient)
	sxutil.SimpleSubscribeDemand(rcmClient, supplyRecommendDemandCallback)
	// go subscribeJsonRecordSupply(envClient)
	http.HandleFunc("/api/v0/want_bus_diagram_adjust", wantBusDiagramAdjustHandler)
	http.HandleFunc("/api/v0/post_bus_diagram_adjust", postBusDiagramAdjustHandler)
	fmt.Println("Server is running on port 8050")
	go http.ListenAndServe(":8050", nil)

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
