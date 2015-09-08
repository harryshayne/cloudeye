package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ActiveState/tail"
	fb "github.com/huandu/facebook"
	"github.com/influxdb/influxdb/client"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"strconv"
	"time"
	"strings"
	//"reflect"
)

type PreTagsStruct struct {
	Host string
	Module string
	Time string
}

type ConfigStruct struct {
	Metricname string   `json:"metricname"`
	Value      string   `json:"value"`
	C_type     string   `json:"type"`
	Tags       []string `json:"tags"`
}

type InfluxdbConf struct {
	Host     string `json:host`
	Port     int    `json:port`
	Database string `json:database`
	User     string `json:user`
	Pwd      string `json:pwd`
}

type ModuleStruct struct {
	Modulename string   `json:"modulename"`
	Metrics          []ConfigStruct `json:"metrics"` 
}

type ConfigArr struct {
	Metrics          []ConfigStruct `json:"metrics"`
	Filepath         string         `json:"filepath"`
	Modulesenable	 []string	`json:"modulesenable"`
	Modules		 []ModuleStruct	`json:"modules"`
	Backend_influxdb InfluxdbConf   `json:"backend_influxdb"`
	con              *client.Client //global conn
}

var config ConfigArr

func write_influxdb(con *client.Client, conf InfluxdbConf, pts []client.Point) {

	bps := client.BatchPoints{
		Points:          pts,
		Database:        conf.Database,
		RetentionPolicy: "default",
	}
	_, err := con.Write(bps)
	if err != nil {
		log.Fatal(err)
	}
}

func init_fluxdb(conf InfluxdbConf) (pcon *client.Client, perr error) {
	u, err := url.Parse(fmt.Sprintf("http://%s:%d", conf.Host, conf.Port))
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	influxconf := client.Config{
		URL:      *u,
		Username: os.Getenv(conf.User),
		Password: os.Getenv(conf.Pwd),
	}

	con, err := client.NewClient(influxconf)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	dur, ver, err := con.Ping()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	log.Printf("Happy as a Hippo! %v, %s", dur, ver)
	return con, nil
}

func readconf(filename string) error {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println("ReadFile: ", err.Error())
		return err
	}
	if err := json.Unmarshal([]byte(bytes), &config); err != nil {
		fmt.Println("Unmarshalin readconf: ", err.Error())
		return err
	}
	return nil
}

func processmetric(c ConfigStruct, log fb.Result, pretag PreTagsStruct) error {
	//fmt.Println("metricname:value|type|@smaple_rate|#tag1:value1,tagO2:value2")
	s := fmt.Sprintf("%s:%f|%s|@smaple_rate|%s|%s", c.Metricname, log[c.Value], c.C_type,pretag.Module,pretag.Host)
	influxtags := make(map[string]string)

	for _, val := range c.Tags {
		//process tag,improve robust
		var ok bool
		influxtags[val],ok = log[val].(string)
		if ok == false{
			fmt.Println("tag is not a string or unexist:",val)
			return nil
		}
		/*if index == 0 {
			t := string("#") + string(val) + string(":") + log[val].(string)
			s += t
		} else {
			t := string(",") + string(val) + string(":") + log[val].(string)
			s += t
		}*/
	}
	influxtags["host"]=pretag.Host
	influxtags["module"]=pretag.Module
	fmt.Println(s)
	/*logtime, err := strconv.ParseInt(log[c.Time].(string), 10, 0)
	if err != nil {
		return errors.New("strconv.ParseInt(log[c.Time] failed")
	}*/
	//process value,improve robust
	if log[c.Value] == nil {
		fmt.Println("value is nil")
		return nil
	}
	//_, ok := log[c.Value].(float32)
	var valued float64
	if isdigit(log[c.Value])==false {
		b,error := strconv.ParseFloat(string(log[c.Value].(string)),64)
		if error!=nil{
			fmt.Println("value can't change to digit")
			return nil
		}
		valued=b
	}else{
		valued=float64(log[c.Value].(float64))
	}
	//cause influxdb client write func will change float64 digit like 18.0 into int64(this is bug),we manually add 0.00001 to that digit,then will always be float64 
	if isint(valued){
		valued+=0.00001
	}
	//format time
	location := time.Now().Location()
  	timeloc, err := ConvStringToTime(pretag.Time, location)
	if err != nil{
		fmt.Println("illegal time format")
		return nil
	}
	p := client.Point{
		Measurement: c.Metricname,
		Tags:        influxtags,
		Fields: map[string]interface{}{
			"value": valued,
		},
		//Time:      time.Unix(int64(log[c.Time].(float64)), 0),
		Time:      *timeloc,
		Precision: "s",
	}
	var pts = make([]client.Point, 1)
	pts[0] = p
	fmt.Println(pts)
	//fmt.Println("p.valued type:",reflect.TypeOf(p.Fields["value"]))
	write_influxdb(config.con, config.Backend_influxdb, pts)
	return nil
}

func isdigit(i interface{}) bool{  
    switch i.(type) {
    case int,int64:
	return true
    case float32,float64:
        return true
    default:
	return false
    }
}

func isint(f float64) bool{
	z:=f-float64(int(f))
	if z==0{
		return true
	}else{
		return false
	}
}


func contains(s []string, q string) bool {
	if s == nil || len(s) == 0 {
		return false
	}
	for _, a := range s {
		if a == q {
			return true
		}
	}
	return false
}

func processprefix(s string) (string,PreTagsStruct) {
	var index int
	var pretag PreTagsStruct
	index = strings.Index(s,"{")
	if index <= 20 {
		//fmt.Println("error in {")
		return "",pretag
	}
	//fmt.Println("index:",index)
	prestr:=s[:index-1]
	sufstr:=s[index:]
	prestrs := strings.Split(prestr,"|")
	if  len(prestrs) <= 3 {
		//fmt.Println("error in |")
		return "",pretag
	}
	//fmt.Println("prestr:", prestr)
	if contains(config.Modulesenable, prestrs[2]) {
		tmp:=strings.Split(prestrs[0],":")
		if len(tmp) <= 3 {
			//fmt.Println("error in :")
			return "",pretag
		}
		pretag.Host = strings.TrimSpace(tmp[len(tmp)-1])
		pretag.Time = prestrs[1]
		pretag.Module = prestrs[2]
	}else{
		//fmt.Println("log not in modules")
		return "",pretag
	}
	return sufstr,pretag
}

func processlog(s string) error {
	var js string
	var pretag PreTagsStruct
	js,pretag=processprefix(s)
	if js == "" {
		//fmt.Println("ignore this log!")
		return nil
	}
	//fmt.Println("pretag:",pretag)
	//fmt.Println("json:",js)
	//fmt.Printf("Host:%v|Time:%v|Module:%v\n", pretag.Host, pretag.Time, pretag.Module)
	
	var r fb.Result
	json.Unmarshal([]byte(js), &r)
	//fmt.Println(r)
	for _, mval := range config.Modules {
		if mval.Modulename == pretag.Module {
			for _, val := range mval.Metrics {
				//fmt.Println(index)
				processmetric(val, r, pretag)
			}		
		}
	}
	//for _, val := range config.Metrics {
		//fmt.Println(index)
	//	processmetric(val, r, pretag)
	//}
	return nil
}

func ConvStringToTime(str string, location *time.Location) (*time.Time, error) {
	length := len(str)
	if length != 14 && length !=8 {
		return nil, errors.New("invalid date time,should be 20150907145712")
	}
	var err error
	y, M, d, h, m, s := 0, 0, 0, 0, 0, 0
	if y, err = strconv.Atoi(str[0:4]); err != nil {
		return nil, err
	}
	if M, err = strconv.Atoi(str[4:6]); err != nil {
		return nil, err
	}
	if d, err = strconv.Atoi(str[6:8]); err != nil {
		return nil, err
	}

	if length == 14 {
		if h, err = strconv.Atoi(str[8:10]); err != nil {
			return nil, err
		}
		if m, err = strconv.Atoi(str[10:12]); err != nil {
			return nil, err
		}
		if s, err = strconv.Atoi(str[12:14]); err != nil {
			return nil, err
		}

	}
	date := time.Date(y, time.Month(M), d, h, m, s, 0, location)
	return &date, nil
}

func main() {
	log.Println("Started!")
	readconf("./conf/parse.conf")
	fmt.Println("config:",config)
	con, err := init_fluxdb(config.Backend_influxdb)
	if err != nil {
		fmt.Println("init_fluxdb error")
	}
	config.con = con
	t, _ := tail.TailFile(config.Filepath, tail.Config{Follow: true})
	for line := range t.Lines {
		processlog(line.Text)
	}
}
