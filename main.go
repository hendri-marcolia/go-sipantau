package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Location struct {
	Nama    string `json:"nama"`
	ID      int    `json:"id"`
	Kode    string `json:"kode"`
	Tingkat int    `json:"tingkat"`
}

type LimitedWaitGroup struct {
	wg    sync.WaitGroup
	limit int
	sem   chan struct{}
}

func NewLimitedWaitGroup(limit int) *LimitedWaitGroup {
	return &LimitedWaitGroup{
		wg:    sync.WaitGroup{},
		limit: limit,
		sem:   make(chan struct{}, limit),
	}
}

func (lwg *LimitedWaitGroup) Add(delta int) {
	if delta > lwg.limit {
		panic("delta larger than limit")
	}
	lwg.wg.Add(delta)
	for i := 0; i < delta; i++ {
		lwg.sem <- struct{}{}
	}
}

func (lwg *LimitedWaitGroup) Done() {
	<-lwg.sem
	lwg.wg.Done()
}

func (lwg *LimitedWaitGroup) Wait() {
	lwg.wg.Wait()
}

const baseURL = "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/"

func main() {
	err := godotenv.Load()
	if err != nil {
		panic("Error loading .env file")
	}

	// Fetch initial JSON
	initialURL := baseURL + "0.json"
	locations, err := fetchLocations(initialURL)
	if err != nil {
		fmt.Println("Error fetching initial locations:", err)
		return
	}

	// Create a channel with buffer to avoid blocking
	dataChannel := make(chan TPSData, 20) // Adjust buffer size as needed

	go insertData(context.Background(), dataChannel)

	// Concurrently process and store locations
	var wg sync.WaitGroup
	for _, loc := range locations {
		wg.Add(1)
		go func(loc Location) {
			defer wg.Done()
			err := processAndStoreLocation(context.Background(), baseURL, loc, dataChannel)
			if err != nil {
				fmt.Println("Error processing and storing location:", err)
			}
		}(loc)
	}
	wg.Wait()
	fmt.Println("All locations processed and stored successfully!")
}

func fetchLocations(url string) ([]Location, error) {
	// fmt.Println("Fetching location : ", url)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var locations []Location
	err = json.Unmarshal(body, &locations)
	if err != nil {
		return nil, err
	}

	return locations, nil
}

func fetchDataTPS(url string) (data TPSData, err error) {
	fmt.Println("Fetching data TPS : ", url)
	resp, err := http.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(body, &data)
	if err != nil {
		return
	}
	return

}

type TPSData struct {
	Id           int64          `json:"id"`
	Mode         string         `json:"mode"`
	Chart        map[string]int `json:"chart"`
	Images       []string       `json:"images"`
	Administrasi Administrasi   `json:"administrasi"`
	PSU          interface{}    `json:"psu"`
	TS           string         `json:"ts"`
	StatusSuara  bool           `json:"status_suara"`
	StatusAdm    bool           `json:"status_adm"`
}

type Administrasi struct {
	SuaraSah        int `json:"suara_sah"`
	SuaraTotal      int `json:"suara_total"`
	PemilihDPTJ     int `json:"pemilih_dpt_j"`
	PemilihDPTL     int `json:"pemilih_dpt_l"`
	PemilihDPTP     int `json:"pemilih_dpt_p"`
	PenggunaDPTJ    int `json:"pengguna_dpt_j"`
	PenggunaDPTL    int `json:"pengguna_dpt_l"`
	PenggunaDPTP    int `json:"pengguna_dpt_p"`
	PenggunaDPTBJ   int `json:"pengguna_dptb_j"`
	PenggunaDPTBL   int `json:"pengguna_dptb_l"`
	PenggunaDPTBP   int `json:"pengguna_dptb_p"`
	SuaraTidakSah   int `json:"suara_tidak_sah"`
	PenggunaTotalJ  int `json:"pengguna_total_j"`
	PenggunaTotalL  int `json:"pengguna_total_l"`
	PenggunaTotalP  int `json:"pengguna_total_p"`
	PenggunaNonDPTJ int `json:"pengguna_non_dpt_j"`
	PenggunaNonDPTL int `json:"pengguna_non_dpt_l"`
	PenggunaNonDPTP int `json:"pengguna_non_dpt_p"`
}

// Function to receive data from channel and insert into MongoDB
func insertData(ctx context.Context, dataChannel <-chan TPSData) error {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(os.Getenv("MONGO_DB_URL")))
	if err != nil {
		fmt.Println("Error connecting to MongoDB:", err)
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer client.Disconnect(context.Background())

	// Database & Collection
	db := client.Database("sipantau")
	collection := db.Collection("data_tps")
	indexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "id", Value: -1}},
		Options: options.Index().SetUnique(true),
	}
	_, err = collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		panic(err)
	}

	// Receive data from channel and insert
	for data := range dataChannel {
		// Access and store relevant data based on your needs

		// Convert data to bson.M for insertion
		// doc, err := bson.M(data)
		// if err != nil {
		// return fmt.Errorf("error converting data to bson.M: %v", err)
		// }
		// fmt.Println("Received data on Channel :", data)

		_, err := collection.InsertOne(ctx, data)
		if err != nil {
			return fmt.Errorf("error inserting document: %v", err)
		}

		// fmt.Printf("Successfully stored data\n")
	}
	fmt.Printf("Ended")

	return nil
}

func fetchAndStoreTPS(ctx context.Context, burl string, loc Location, dataChannel chan TPSData) error {
	// Store the current location in MongoDB
	url := burl + loc.Kode + ".json"
	subLocations, err := fetchLocations(url)
	if err != nil {
		return err
	}

	// Concurrently process and store sub-locations
	wg2 := NewLimitedWaitGroup(1)
	for _, subLoc := range subLocations {
		wg2.Add(1)
		go func(subLoc Location) {
			defer wg2.Done()
			data, err := fetchDataTPS(strings.TrimRight(strings.ReplaceAll(url, "wilayah/pemilu/ppwp", "pemilu/hhcw/ppwp"), ".json") + "/" + subLoc.Kode + ".json")
			if err != nil {
				fmt.Println("Error processing TPS:", subLoc.Kode, err)
			}
			data.Id, _ = strconv.ParseInt(subLoc.Kode, 10, 64)
			if data.StatusSuara {
				dataChannel <- data
			}
		}(subLoc)

	}

	return nil
}

func processAndStoreLocation(ctx context.Context, burl string, loc Location, dataChannel chan TPSData) error {
	// Fetch JSON for the current location
	url := burl + loc.Kode + ".json"
	subLocations, err := fetchLocations(url)
	if err != nil {
		return err
	}

	// Concurrently process and store sub-locations
	wg := NewLimitedWaitGroup(1)
	for _, subLoc := range subLocations {
		wg.Add(1)
		go func(subLoc Location) {
			defer wg.Done()
			fmt.Println("Processing : ", url)
			if subLoc.Tingkat == 4 {
				err = fetchAndStoreTPS(ctx, strings.TrimRight(url, ".json")+"/", subLoc, dataChannel)
			} else {
				err = processAndStoreLocation(ctx, strings.TrimRight(url, ".json")+"/", subLoc, dataChannel)
			}
			if err != nil {
				fmt.Println("Error processing and storing sub-location:", err)
			}
		}(subLoc)
	}
	wg.Wait()

	return nil
}
