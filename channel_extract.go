package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	sxmqtt "github.com/synerex/proto_mqtt"
	proto_wes "github.com/synerex/proto_wes"

	sxutil "github.com/synerex/synerex_sxutil"
)

// this provider just extract .csv file into informations.

var (
	channel   = flag.String("channel", "3", "Retrieving channel type(default 3, support comma separated)")
	local     = flag.String("local", "", "Specify Local Synerex Server")
	extfile   = flag.String("file", "", "Extract file name") // only one file
	startDate = flag.String("startDate", "02-07", "Specify Start Date")
	endDate   = flag.String("endDate", "12-31", "Specify End Date")
	startTime = flag.String("startTime", "00:00", "Specify Start Time")
	endTime   = flag.String("endTime", "24:00", "Specify End Time")
	dir       = flag.String("dir", "store", "Directory of data storage")   // for all file
	all       = flag.Bool("all", true, "Extract all file in data storage") // for all file
	verbose   = flag.Bool("verbose", false, "Verbose information")
	jst       = flag.Bool("jst", false, "Run/display with JST Time")
	pose      = flag.Bool("pose", false, "Print only pose")
)

func init() {

}

const dateFmt = "2006-01-02T15:04:05.999Z"

// pose message
type PoseMes struct {
	Header struct {
		Stamp struct {
			Secs  int `json:"secs"`
			Nsecs int `json:"nsecs"`
		} `json:"stamp"`
		FrameID interface{} `json:"frame_id"`
		Seq     int         `json:"seq"`
	} `json:"header"`
	Pose struct {
		Position struct {
			X float64 `json:"x"`
			Y float64 `json:"y"`
			Z float64 `json:"z"`
		} `json:"position"`
		Orientation struct {
			X float64 `json:"x"`
			Y float64 `json:"y"`
			Z float64 `json:"z"`
			W float64 `json:"w"`
		} `json:"orientation"`
	} `json:"pose"`
}

func atoUint(s string) uint32 {
	r, err := strconv.Atoi(s)
	if err != nil {
		log.Print("err", err)
	}
	return uint32(r)
}

func getHourMin(dt string) (hour int, min int) {
	st := strings.Split(dt, ":")
	hour, _ = strconv.Atoi(st[0])
	min, _ = strconv.Atoi(st[1])
	return hour, min
}

func getMonthDate(dt string) (month int, date int) {
	st := strings.Split(dt, "-")
	month, _ = strconv.Atoi(st[0])
	date, _ = strconv.Atoi(st[1])
	return month, date
}

// sending People Counter File.
func sendingStoredFile(clients map[int]bool) {
	// file
	fp, err := os.Open(*extfile)
	if err != nil {
		panic(err)
	}
	defer fp.Close()

	scanner := bufio.NewScanner(fp) // csv reader
	var buf []byte = make([]byte, 1024)
	scanner.Buffer(buf, 1024*1024*64) // 64Mbytes buffer

	started := false // start flag
	stHour, stMin := getHourMin(*startTime)
	edHour, edMin := getHourMin(*endTime)

	if *verbose {
		log.Printf("Verbose output for file %s", *extfile)
		log.Printf("StartTime %02d:%02d  -- %02d:%02d", stHour, stMin, edHour, edMin)
	}
	jstZone := time.FixedZone("Asia/Tokyo", 9*60*60)

	for scanner.Scan() { // read one line.

		dt := scanner.Text()
		//		if *verbose {
		//			log.Printf("Scan:%s", dt)
		//		}

		token := strings.Split(dt, ",")
		//		log.Printf("dt:%d, token %d", len(dt), len(token))

		//                                                , 0  ,1    ,2          ,3           ,4              ,5            ,6           , 7        ,8
		//line := fmt.Sprintf("%s,%d,%d,%d,%d,%s,%s,%d,%s", ts, sm.Id, sm.SenderId, sm.TargetId, sm.ChannelType, sm.SupplyName, sm.ArgJson, sm.MbusId, bsd)

		tm, err := time.Parse(dateFmt, token[0]) // RFC3339Nano
		if err != nil {
			log.Printf("Time parsing error with %s, %s : %v", token[0], dt, err)
		}

		if *jst { // we need to convert UTC to JST.
			tm = tm.In(jstZone)
		}

		//		tp, _ := ptypes.TimestampProto(tm)

		if !started {
			if (tm.Hour() > stHour || (tm.Hour() == stHour && tm.Minute() >= stMin)) &&
				(tm.Hour() < edHour || (tm.Hour() == edHour && tm.Minute() <= edMin)) {
				started = true
				log.Printf("Start output! %v", tm)
			} else {
				continue // skip all data
			}
		} else {
			if tm.Hour() > edHour || (tm.Hour() == edHour && tm.Minute() > edMin) {
				started = false
				log.Printf("Stop  output! %v", tm)
				continue
			}
		}

		if !started {
			continue // skip following
		}

		sDec, err2 := base64.StdEncoding.DecodeString(token[8])
		if err2 != nil {
			log.Printf("Decoding error with %s : %v", token[8], err)
		}

		{ // extract each packets
			// if channel in channels
			chnum, err := strconv.Atoi(token[4])
			_, ok := clients[chnum]
			if ok && err == nil { // if there is channel
				//				_, nerr := NotifySupplyWithTime(client, &smo, tsProto)
				//				log.Printf("Sent OK! %#v\n", smo)
				//				fmt.Printf("ts:%s,chan:%s,%s,%s,%s,len:%d", tm.Format(time.RFC3339), token[4], token[5], token[6], token[7], len(token[8]))
				if chnum == 16 { //MQTT
					mq := &sxmqtt.MQTTRecord{}
					err := proto.Unmarshal(sDec, mq)
					if err == nil {
						//						fmt.Printf("MQTT:%v\n", mq)
						//						fmt.Printf("MQTT:%s: %s\n", mq.Topic, string(mq.Record))

						if strings.HasSuffix(mq.Topic, "pose") {
							pose := &PoseMes{}
							jerr := json.Unmarshal(mq.Record, pose)
							if jerr != nil {
								log.Printf("%s:JSON:%s\n", mq.Topic, string(mq.Record))
								log.Fatal("Err! on json decode! %v", jerr)
							} else {
								//						fmt.Printf("MQTT:%s: %s\n", mq.Topic, string(mq.Record))
								fmt.Printf("%s, %s, %.11f, %.11f, %.11f, %.11f, %.11f, %.11f, %.11f\n", tm.Format(time.RFC3339),
									mq.Topic, pose.Pose.Position.X, pose.Pose.Position.Y, pose.Pose.Position.Z,
									pose.Pose.Orientation.X, pose.Pose.Orientation.Y, pose.Pose.Orientation.Z, pose.Pose.Orientation.W)
								//		fmt.Printf("%s\n", string(mq.Record))

							}
						} else {
							if !*pose {
								fmt.Printf("%s, %s, %s\n", tm.Format(time.RFC3339), mq.Topic, string(mq.Record))
							}
						}
					}
				} else {
					fmt.Printf("ts:%s,chan:%s,%s,%s,%s,len[%d]", tm.Format(time.RFC3339), token[4], token[5], token[6], token[7], len(token[8]))

					wes := &proto_wes.WesMessage{}
					err := proto.Unmarshal(sDec, wes)
					if err == nil {
						fmt.Printf("WES:%v\n", wes)
					}
				}

				//			cont := pb.Content{Entity: sDec}
				//			smo := sxutil.SupplyOpts{
				//				Name:  token[5],
				//				JSON:  token[6],
				//				Cdata: &cont,
				//			}

			}
		}

		serr := scanner.Err()
		if serr != nil {
			log.Printf("Scanner error %v", serr)
		}
	}
}

func sendAllStoredFile(clients map[int]bool) {
	// check all files in dir.
	stMonth, stDate := getMonthDate(*startDate)
	edMonth, edDate := getMonthDate(*endDate)

	if *dir == "" {
		log.Printf("Please specify directory")
		data := "data"
		dir = &data
	}
	files, err := ioutil.ReadDir(*dir)

	if err != nil {
		log.Printf("Can't open diretory %v", err)
		os.Exit(1)
	}
	// should be sorted.
	var ss = make(sort.StringSlice, 0, len(files))

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".csv") { // check is CSV file
			//
			fn := file.Name()
			var year, month, date int
			ct, err := fmt.Sscanf(fn, "%4d-%02d-%02d.csv", &year, &month, &date)
			if (month > stMonth || (month == stMonth && date >= stDate)) &&
				(month < edMonth || (month == edMonth && date <= edDate)) {
				ss = append(ss, file.Name())
			} else {
				log.Printf("file: %d %v %s: %04d-%02d-%02d", ct, err, fn, year, month, date)
			}
		}
	}

	ss.Sort()

	for _, fname := range ss {
		dfile := path.Join(*dir, fname)
		// check start date.

		log.Printf("Sending %s", dfile)
		extfile = &dfile
		sendingStoredFile(clients)
	}

}

//dataServer(pc_client)

func main() {
	log.Printf("ChannelExtract(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	//	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	// check channel types.
	//
	chans := strings.Split(*channel, ",")
	pcClients := make(map[int]bool)
	for _, ch := range chans {
		v, err := strconv.Atoi(ch)
		if err == nil {
			pcClients[v] = true
		} else {
			log.Fatal("Can't convert channels ", *channel)
		}
	}

	if *extfile != "" {
		//		for { // infinite loop..
		sendingStoredFile(pcClients)
		//		}
	} else if *all { // send all file
		sendAllStoredFile(pcClients)
	} else if *dir != "" {
	}

	//	wg.Wait()

}
