package main

import (
	"encoding/json"
	//"errors"
	"fmt"
	"github.com/ActiveState/tail"
	fb "github.com/huandu/facebook"
	"github.com/influxdb/influxdb/client"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	//"strconv"
	"time"
	"strings"
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

type ConfigArr struct {
	Metrics          []ConfigStruct `json:"metrics"`
	Filepath         string         `json:"filepath"`
	Modules		 []string	`json:"modules"`
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
	s := fmt.Sprintf("%s:%f|%s|@smaple_rate|", c.Metricname, log[c.Value], c.C_type)
	influxtags := make(map[string]string)

	for _, val := range c.Tags {
		var ok bool
		influxtags[val],ok = log[val].(string)
		if ok == false{
			fmt.Println("tag is not a string:",val)
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
	if log[c.Value] == nil {
		fmt.Println("value is nil")
		return nil
	}
	p := client.Point{
		Measurement: c.Metricname,
		Tags:        influxtags,
		Fields: map[string]interface{}{
			"value": log[c.Value],
		},
		//Time:      time.Unix(int64(log[c.Time].(float64)), 0),
		Time:      time.Now(),
		Precision: "s",
	}
	var pts = make([]client.Point, 1)
	pts[0] = p
	fmt.Println(pts)
	write_influxdb(config.con, config.Backend_influxdb, pts)
	return nil
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
	if contains(config.Modules, prestrs[2]) {
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
	for _, val := range config.Metrics {
		//fmt.Println(index)
		processmetric(val, r, pretag)
	}
	return nil
}

func main() {
	log.Println("Started!")
	readconf("./conf/parse.conf")
	fmt.Println(config)
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
