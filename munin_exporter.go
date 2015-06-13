package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	proto         = "tcp"
	retryInterval = 1
)

var (
	listeningAddress    = flag.String("listeningAddress", ":8080", "Address on which to expose Prometheus metrics.")
	muninAddress        = flag.String("muninAddress", "localhost:4949", "munin-node address.")
	muninScrapeInterval = flag.Int("muninScrapeInterval", 60, "Interval in seconds between scrapes.")
	globalConn          net.Conn
	hostname            string
	graphs              []string
	gaugePerMetric      map[string]*prometheus.GaugeVec
	muninBanner         *regexp.Regexp
)

func init() {
	flag.Parse()
	var err error
	gaugePerMetric = map[string]*prometheus.GaugeVec{}
	muninBanner = regexp.MustCompile(`# munin node at (.*)`)

	err = connect()
	if err != nil {
		log.Fatalf("Could not connect to %s: %s", *muninAddress, err)
	}
}

func serveStatus() {
	http.Handle("/metrics", prometheus.Handler())
	http.ListenAndServe(*listeningAddress, nil)
}

func connect() (err error) {
	log.Printf("Connecting...")
	globalConn, err = net.Dial(proto, *muninAddress)
	if err != nil {
		return
	}
	log.Printf("connected!")

	reader := bufio.NewReader(globalConn)
	head, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	matches := muninBanner.FindStringSubmatch(head)
	if len(matches) != 2 { // expect: # munin node at <hostname>
		return fmt.Errorf("Unexpected line: %s", head)
	}
	hostname = matches[1]
	log.Printf("Found hostname: %s", hostname)
	return
}

func muninCommand(cmd string) (reader *bufio.Reader, err error) {
	reader = bufio.NewReader(globalConn)

	fmt.Fprintf(globalConn, cmd+"\n")

	_, err = reader.Peek(1)
	switch err {
	case io.EOF:
		log.Printf("not connected anymore, closing connection")
		globalConn.Close()
		for {
			err = connect()
			if err == nil {
				break
			}
			log.Printf("Couldn't reconnect: %s", err)
			time.Sleep(retryInterval * time.Second)
		}

		return muninCommand(cmd)
	case nil: //no error
		break
	default:
		log.Fatalf("Unexpected error: %s", err)
	}

	return
}

func muninList() (items []string, err error) {
	munin, err := muninCommand("list")
	if err != nil {
		log.Printf("couldn't get list")
		return
	}

	response, err := munin.ReadString('\n') // we are only interested in the first line
	if err != nil {
		log.Printf("couldn't read response")
		return
	}

	if response[0] == '#' { // # not expected here
		err = fmt.Errorf("Error getting items: %s", response)
		return
	}
	items = strings.Fields(strings.TrimRight(response, "\n"))
	return
}

func muninConfig(name string) (config map[string]map[string]string, graphConfig map[string]string, err error) {
	graphConfig = make(map[string]string)
	config = make(map[string]map[string]string)

	resp, err := muninCommand("config " + name)
	if err != nil {
		log.Printf("couldn't get config for %s", name)
		return
	}

	for {
		line, err := resp.ReadString('\n')
		if err == io.EOF {
			log.Fatalf("unexpected EOF, retrying")
			return muninConfig(name)
		}
		if err != nil {
			return nil, nil, err
		}
		if line == ".\n" { // munin end marker
			break
		}
		if line[0] == '#' { // here it's just a comment, so ignore it
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			return nil, nil, fmt.Errorf("Line unexpected: %s", line)
		}
		key, value := parts[0], strings.TrimRight(strings.Join(parts[1:], " "), "\n")

		key_parts := strings.Split(key, ".")
		if len(key_parts) > 1 { // it's a metric config (metric.label etc)
			if _, ok := config[key_parts[0]]; !ok { //FIXME: is there no better way?
				config[key_parts[0]] = make(map[string]string)
			}
			config[key_parts[0]][key_parts[1]] = value
		} else {
			graphConfig[key_parts[0]] = value
		}
	}
	return
}

func registerMetrics() (err error) {
	items, err := muninList()
	if err != nil {
		return
	}

	for _, name := range items {
		graphs = append(graphs, name)
		configs, graphConfig, err := muninConfig(name)
		if err != nil {
			return err
		}

		for metric, config := range configs {
			metricName := strings.Replace(name + "_" + metric, "-","_",-1)
			desc := graphConfig["graph_title"] + ": " + config["label"]
			if config["info"] != "" {
				desc = desc + ", " + config["info"]
			}
			gv := prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: metricName,
					Help: desc,
				},
				[]string{"hostname"},
			)
			log.Printf("Registered %s: %s", metricName, desc)
			gaugePerMetric[metricName] = gv
			prometheus.Register(gv)
		}
	}
	return nil
}

func fetchMetrics() (err error) {
	for _, graph := range graphs {
		munin, err := muninCommand("fetch " + graph)
		if err != nil {
			return err
		}

		for {
			line, err := munin.ReadString('\n')
			line = strings.TrimRight(line, "\n")
			if err == io.EOF {
				log.Fatalf("unexpected EOF, retrying")
				return fetchMetrics()
			}
			if err != nil {
				return err
			}
			if len(line) == 1 && line[0] == '.' {
				log.Printf("End of list")
				break
			}

			parts := strings.Fields(line)
			if len(parts) != 2 {
				log.Printf("unexpected line: %s", line)
				continue
			}
			key, value_s := strings.Split(parts[0], ".")[0], parts[1]
			value, err := strconv.ParseFloat(value_s, 64)
			if err != nil {
				log.Printf("Couldn't parse value in line %s, malformed?", line)
				continue
			}
			name := strings.Replace(graph + "_" + key, "-","_",-1)
			log.Printf("%s: %f\n", name, value)
			gaugePerMetric[name].WithLabelValues(hostname).Set(value)
		}
	}
	return
}

func main() {
	flag.Parse()
	err := registerMetrics()
	if err != nil {
		log.Fatalf("Could not register metrics: %s", err)
	}

	go serveStatus()

	func() {
		for {
			log.Printf("Scraping")
			err := fetchMetrics()
			if err != nil {
				log.Printf("Error occured when trying to fetch metrics: %s", err)
			}
			time.Sleep(time.Duration(*muninScrapeInterval) * time.Second)
		}
	}()
}
