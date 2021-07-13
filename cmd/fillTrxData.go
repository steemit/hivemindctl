package cmd

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"encoding/json"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var wg sync.WaitGroup
var db *gorm.DB
var waitInsertData []HiveTrxidBlockNum
var apiUrl string
var failedTask []string
var retryTimes int = 0

func init() {
	rootCmd.AddCommand(fillTrxData)
}

var fillTrxData = &cobra.Command{
	Use:   "fill_trx_data",
	Short: "This command will fill in the missing trx_id data.",
	Run: func(cmd *cobra.Command, args []string) {
		apiUrl = viper.Get("API_URL").(string)
		pgDsn := viper.Get("PG_DSN").(string)
		if pgDsn == "" {
			fmt.Println("need PG_DSN config.")
			os.Exit(1)
		}
		fmt.Println(pgDsn)
		var err error
		db, err = gorm.Open(postgres.Open(pgDsn), &gorm.Config{
			Logger:          logger.Default.LogMode(logger.Silent),
			CreateBatchSize: 1000,
		})
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		latestBlockNumInDb := getLatestBlockNumFromDb()
		fmt.Println("Latest Block Num In Db: ", latestBlockNumInDb)

		var lostBlockNum []string
		findLostBlock(latestBlockNumInDb, &lostBlockNum)
		fmt.Println("Finish finding.")

		// Get Block and Insert Data
		processStep := viper.Get("PROCESS_STEP").(int)
		for retryTimes == 0 || len(failedTask) > 0 {
			if retryTimes > 0 {
				lostBlockNum = make([]string, len(failedTask))
				copy(lostBlockNum, failedTask)
				failedTask = make([]string, 0)
				fmt.Println("Failed Tasks: ", len(lostBlockNum))
			}
			temp := 1
			lenOfLostBlock := len(lostBlockNum)
			for i, blockNum := range lostBlockNum {
				if temp == 1 {
					if i+processStep <= lenOfLostBlock {
						// fmt.Println("add", processStep)
						wg.Add(processStep)
					} else {
						// fmt.Println("add", lenOfLostBlock-i)
						wg.Add(lenOfLostBlock - i)
					}
				}
				// fmt.Println("go run: ", blockNum)
				go getBlock(blockNum)
				if math.Mod(float64(temp), float64(processStep)) == 0.0 {
					// fmt.Println("wait")
					wg.Wait()
					insertBulkData(&waitInsertData)
					waitInsertData = waitInsertData[:0]
					temp = 1
				} else {
					temp++
				}

			}
			fmt.Println("wait out")
			wg.Wait()
			insertBulkData(&waitInsertData)
			waitInsertData = waitInsertData[:0]
			retryTimes++
		}
	},
}

func getLatestBlockNumFromDb() int64 {
	var result HiveTrxidBlockNum
	db.Select("block_num").Order("block_num desc").First(&result)
	return result.BlockNum
}

func findLostBlock(latestBlockNum int64, result *[]string) {
	var (
		blockNum  int64 = 1
		upper     int64
		tmpResult []HiveTrxidBlockNum
		step      int64 = int64(viper.Get("SEARCH_STEP").(int))
	)
	for blockNum < latestBlockNum {
		// fmt.Println("start loop: ", blockNum, latestBlockNum)
		if blockNum+step > latestBlockNum {
			if upper == latestBlockNum-1 {
				return
			}
			upper = latestBlockNum - 1
		} else {
			upper = blockNum + step
		}
		searching := makeRange(blockNum, upper)
		fmt.Printf("Range From %v to %v\n", blockNum, upper)
		whereStr := fmt.Sprintf("block_num in (%s)", strings.Join(searching, ","))
		db.Select("block_num").Where(whereStr).Group("block_num").Find(&tmpResult)
		// fmt.Println("length of tmpResult:", len(tmpResult))
		if len(tmpResult) > 0 {
			tmp := make(map[string]int)
			final := make([]string, 0)
			for _, row := range tmpResult {
				tmp[strconv.FormatInt(row.BlockNum, 10)]++
			}
			for _, searchingBlockNum := range searching {
				tmp[searchingBlockNum]++
			}
			for searchingBlockNum, count := range tmp {
				if count == 1 {
					final = append(final, searchingBlockNum)
				}
			}
			searching = final
		}
		*result = append(*result, searching...)
		blockNum = upper + 1
	}
}

func makeRange(min int64, max int64) []string {
	a := make([]string, max-min+1)
	for i := range a {
		a[i] = strconv.FormatInt(min+int64(i), 10)
	}
	return a
}

func getBlock(blockNum string) {
	defer wg.Done()
	var jsonData = []byte(fmt.Sprintf(`{"jsonrpc":"2.0", "method":"condenser_api.get_block", "params":[%s], "id":1}`, blockNum))
	request, _ := http.NewRequest("POST", apiUrl, bytes.NewBuffer(jsonData))
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")

	client := &http.Client{}
	response, error := client.Do(request)
	if error != nil {
		fmt.Println("Error:", error, blockNum)
		failedTask = append(failedTask, blockNum)
		return
	}
	defer response.Body.Close()
	if response.Status != "200 OK" {
		return
	}
	body, _ := ioutil.ReadAll(response.Body)
	var res SteemResponse
	json.Unmarshal(body, &res)
	tmpBlockNum, _ := strconv.ParseInt(blockNum, 10, 64)
	if len(res.Result.TransactionIds) == 0 {
		waitInsertData = append(waitInsertData, HiveTrxidBlockNum{nil, tmpBlockNum})
		return
	}
	for _, trxId := range res.Result.TransactionIds {
		waitInsertData = append(waitInsertData, HiveTrxidBlockNum{&trxId, tmpBlockNum})
	}
}

func insertBulkData(data *[]HiveTrxidBlockNum) {
	if len(*data) == 0 {
		return
	}
	fmt.Println("insert from block num: ", (*data)[0].BlockNum)
	db.Create(data)
}

// ------ Struct -------

type HiveTrxidBlockNum struct {
	TrxId    *string `json:"trx_id"`
	BlockNum int64   `json:"block_num"`
}

func (HiveTrxidBlockNum) TableName() string {
	return "hive_trxid_block_num"
}

type SteemResponse struct {
	Jsonrpc string     `json:"jsonrpc"`
	Result  SteemBlock `json:"result"`
	Id      int32      `json:"id"`
}

type SteemBlock struct {
	TransactionMerkleRoot string             `json:"transaction_merkle_root"`
	Previous              string             `json:"previous"`
	Timestamp             string             `json:"timestamp"`
	Witness               string             `json:"witness"`
	Extensions            string             `json:"extensions"`
	WitnessSignature      string             `json:"witness_signature"`
	Transactions          []SteemTransaction `json:"transactions"`
	TransactionIds        []string           `json:"transaction_ids"`
	BlockId               string             `json:"block_id"`
	SigningKey            string             `json:"signing_key"`
}

type SteemTransaction struct {
	RefBlockNum    int      `json:"ref_block_num"`
	RefBlockPrefix int64    `json:"ref_block_prefix"`
	Expiration     string   `json:"expiration"`
	Operations     []string `json:"operations"`
	Extensions     []string `json:"extensions"`
	Signatures     []string `json:"Signatures"`
	TransactionId  string   `json:"transaction_id"`
	BlockNum       int64    `json:"block_num"`
	TransactionNum int      `json:"transaction_num"`
}
