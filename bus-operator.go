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
	num                  = flag.Int("num", 1, "Number of BusOperator")
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
	wantBusAddTemp       = false
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

type BusCanAdd struct {
	Want        bool   `json:"want"`
	FromStation string `json:"from_station"`
	ToStation   string `json:"to_station"`
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

			wantBusAddTemp = true
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

// シミュレータから Synerex のバス運行会社に臨時便を調達できるかどうかを返してもらう
func postBusAddTempHandler(w http.ResponseWriter, r *http.Request) {
	busstop := r.URL.Query().Get("busstop")
	isUp := r.URL.Query().Get("is_up") == "True"
	isStartingPoint := r.URL.Query().Get("is_starting_point") == "True"
	travelTimeStr := r.URL.Query().Get("travel_time")
	travelTime, err := strconv.Atoi(travelTimeStr)
	if err != nil {
		log.Printf("Error in travelTimeStr: %s, err: %v\n", travelTimeStr, err)
	}
	line := r.URL.Query().Get("line")
	end := r.URL.Query().Get("end")
	arrivalTimeStr := r.URL.Query().Get("arrival_time")
	arrivalTime, err := strconv.Atoi(arrivalTimeStr)
	if err != nil {
		log.Printf("Error in arrivalTimeStr: %s, err: %v\n", arrivalTimeStr, err)
	}
	next := r.URL.Query().Get("next")
	departureTimeStr := r.URL.Query().Get("departure_time")
	departureTime, err := strconv.Atoi(departureTimeStr)
	if err != nil {
		log.Printf("Error in departureTimeStr: %s, err: %v\n", departureTimeStr, err)
	}
	busIDStr := r.URL.Query().Get("bus_id")
	busID, err := strconv.Atoi(busIDStr)
	if err != nil {
		log.Printf("Error in busIDStr: %s, err: %v\n", busIDStr, err)
	}

	spo := sxutil.SupplyOpts{
		Name: role,
		JSON: fmt.Sprintf(
			`{ "type": "%s", "vehicle": "マイクロバス", "date": "ASAP", "from": "%s", "to": "%s", "stops": "none", "way": "round-trip", "repetition": 4, "isUp": "%t", "isStartingPoint": "%t", "travelTime": %d, "line": "%s", "end": "%s", "arrivalTime": %d, "departureTime": %d, "busID": %d }`,
			臨時便, busstop, next, isUp, isStartingPoint, travelTime, line, end, arrivalTime, departureTime, busID,
		),
	}
	spid := rcmClient.ProposeSupply(&spo)
	proposedSpIds = append(proposedSpIds, spid)
	log.Printf("#2 ProposeSupply OK! spo: %#v, spid: %d\n", spo, spid)

	status := BusCanAdd{Want: false, FromStation: busstop, ToStation: next}

	log.Printf("Called /api/v0/post_bus_can_add_temp -> Response: %+v\n", status)
	response, err := json.Marshal(status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// Synerex のバス運行会社が臨時便を調達したいかどうかをシミュレータに返す
func wantBusAddTempHandler(w http.ResponseWriter, r *http.Request) {
	status := BusCanAdd{Want: false}
	if wantBusAddTemp {
		status.Want = true
		status.FromStation = "B"
		status.ToStation = "C"
		wantBusAddTemp = false
	}

	log.Printf("Called /api/v0/want_bus_can_add_temp -> Response: %+v\n", status)
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
	log.Printf("%s%d(%s) built %s sha1 %s", role, *num, sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	channelTypes := []uint32{pbase.ALT_PT_SVC} //, pbase.JSON_DATA_SVC}

	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, fmt.Sprintf("%s%d", role, *num), channelTypes, nil)

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

	rcmClient = sxutil.NewSXServiceClient(client, pbase.ALT_PT_SVC, fmt.Sprintf("{Client:%s%d}", role, *num))
	// envClient := sxutil.NewSXServiceClient(client, pbase.JSON_DATA_SVC, fmt.Sprintf("{Client:%s%d}", role, *num))

	wg.Add(1)
	log.Print("Subscribe Supply")
	go subscribeRecommendSupply(rcmClient)
	sxutil.SimpleSubscribeDemand(rcmClient, supplyRecommendDemandCallback)
	// go subscribeJsonRecordSupply(envClient)
	http.HandleFunc("/api/v0/want_bus_diagram_adjust", wantBusDiagramAdjustHandler)
	http.HandleFunc("/api/v0/post_bus_diagram_adjust", postBusDiagramAdjustHandler)
	http.HandleFunc("/api/v0/want_bus_add_temp", wantBusAddTempHandler)
	http.HandleFunc("/api/v0/post_bus_add_temp", postBusAddTempHandler)
	fmt.Printf("Server is running on port 805%d\n", *num)
	go http.ListenAndServe(fmt.Sprintf(":805%d", *num), nil)

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
