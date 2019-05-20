package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"time"

	"github.com/lib/pq"
)

func waitForNotification(l *pq.Listener) {
	for {
		select {
		case n := <-l.Notify:
			fmt.Println("Received data from channel [", n.Channel, "] :")
			// Prepare notification payload for pretty print
			var prettyJSON bytes.Buffer
			err := json.Indent(&prettyJSON, []byte(n.Extra), "", "\t")
			if err != nil {
				fmt.Println("Error processing JSON: ", err)
				return
			}
			fmt.Println(string(prettyJSON.Bytes()))

			var dat map[string]interface{}

			if err := json.Unmarshal(prettyJSON.Bytes(), &dat); err != nil {
				panic(err)
			}

			table := dat["table"].(string)


			botKey := os.Getenv("TGM_BOT_KEY") 
			chatID := os.Getenv("TGM_CHAT_ID") 
			chatMessagesID := os.Getenv("TGM_MESSAGE_CHAT_ID") 
			addr := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage",
				botKey)

			smsAddr := os.Getenv("SMS_GATEWAY_URL") 
			site := os.Getenv("SITE") 
			
			if table == "logins" {
				data := dat["data"].(map[string]interface{})
				login := data["login"].(string)
				rawCode := data["code"]
				if rawCode != nil {
					code := rawCode.(string)
					fmt.Println(login, code)
					
					text := ""

					if len(code) == 4 {
					    text = fmt.Sprintf("Код для логина %s: %s", login, code)
					} else {
					    text = fmt.Sprintf("Ссылка для логина %s: %s/register/%s", login, site, code)
					}
					_, err := http.PostForm(addr,
						url.Values{"chat_id": {chatID}, "text": {text}})
					if err != nil {
						fmt.Println("ERROR", err)
						return
					}
					fmt.Println(addr, chatID, text)
					
				}
			}

			if table == "message" {
				data := dat["data"].(map[string]interface{})
				ID := data["id"].(string)
				rawJ := data["j"]
				if rawJ != nil {
					j := rawJ.(map[string]interface{})
					rawMessage := j["message"]
					if rawMessage != nil {
						message := rawMessage.(string)
						fmt.Println(ID, message)

						text := fmt.Sprintf("Получено сообщение %s: %s", ID, message)

						_, err := http.PostForm(addr,
							url.Values{"chat_id": {chatMessagesID}, "text": {text}})
						if err != nil {
							fmt.Println("ERROR", err)
							return
						}
					}
				}
			}

			if table == "SMS" {
				business_id := ""
				rawBusiness_id := dat["business_id"]
				business_id, success := rawBusiness_id.(string)
				if !success {
					fmt.Println("business_id is null")
				}
				phone := dat["phone"].(string)
				text := dat["text"].(string)
				record_id := ""
				rawRecord_id := dat["record_id"]
				record_id, success = rawRecord_id.(string)
				if !success {
					fmt.Println("record_id is null")
				}
				var Url *url.URL
				Url, err := url.Parse(smsAddr)
				if err != nil {
					fmt.Println("smsAddr: ", smsAddr)
					fmt.Println("err: ", err)
					panic("Bad SMS gateway")
				}
				parameters := url.Values{}
				parameters.Add("business_id", business_id)
				parameters.Add("phone", phone)
				parameters.Add("text", text)
				parameters.Add("record_id", record_id)
				var time float64
				rawTime := dat["time"]
				time, success = rawTime.(float64)
				if success {
					parameters.Add("time", fmt.Sprintf("%.0f", time))
				} else {
					fmt.Println(time)
					fmt.Println(rawTime)
					fmt.Println("time is null")
				}
				Url.RawQuery = parameters.Encode()
	
				fmt.Println(Url.String())

				_, err = http.Get(Url.String())
				if err != nil {
					fmt.Println("ERROR", err)
					return
				}
			}

			return
		case <-time.After(60 * 60 * time.Second):
			fmt.Println("Received no events for 1 hour, checking connection")
			go func() {
				l.Ping()
			}()
			return
		}
	}
}

func main() {

	fmt.Printf("OS: %s\nArchitecture: %s\n", runtime.GOOS, runtime.GOARCH)

	conninfo := fmt.Sprintf("user=%s password=%s host=%s dbname=%s", os.Getenv("PGUSER"), os.Getenv("PGPASSWORD"), os.Getenv("PGHOST"), os.Getenv("PGDATABASE"))
	channel := os.Getenv("PG_CHANNEL")

	fmt.Printf("conn: %s\nchannel: %s\n", conninfo, channel)

	_, err := sql.Open("postgres", conninfo)
	if err != nil {
		panic(err)
	}

	fmt.Printf("connected\n")

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	listener := pq.NewListener(conninfo, 10*time.Second, time.Minute, reportProblem)
	err = listener.Listen(channel)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Start monitoring PostgreSQL channel [%s]...\n", channel)
	for {
		waitForNotification(listener)
	}
}
