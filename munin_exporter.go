package main

import (
	"bufio"
	"sync"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"runtime"
	"github.com/juju/loggo"
	"github.com/prometheus/client_golang/prometheus"
)

var logger = loggo.GetLogger("main")
var rootLogger = loggo.GetLogger("")

const (
	proto           = "tcp"
	retryInterval   = 1
	version_string	= "Munin Exporter version 0.2.1"
	version_num		= "0.2.1"
	revision		= "0.2.1"
)

var (
	listeningAddress    = flag.String("listeningAddress", ":8080", "Address on which to expose Prometheus metrics.")
	listeningPath       = flag.String("listeningPath", "/metrics", "Path on which to expose Prometheus metrics.")
	muninAddress        = flag.String("muninAddress", "localhost:4949", "munin-node address.")
	muninScrapeInterval = flag.Int("muninScrapeInterval", 60, "Interval in seconds between scrapes.")
	logLevel            = flag.String("logLevel", "INFO", "TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL")
	version             = flag.Bool("version", false, "Show application version")
	globalConn          net.Conn
	hostname            string
	graphs              []string
	gaugePerMetric      map[string]*prometheus.GaugeVec
	counterPerMetric    map[string]*muninCounter
	muninBanner         *regexp.Regexp
	wg					= &sync.WaitGroup{}
)

type muninCounter struct {
	counterDesc   *prometheus.Desc
	value         float64
	currentLabels []string

}


func (c *muninCounter) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.counterDesc
}

func (c *muninCounter) Collect(ch chan<- prometheus.Metric) {
	if len(c.currentLabels) == 0 {
		c.currentLabels = []string{"ThisMunin", "Plugin", "IsBroken"}
	}
	ch <- prometheus.MustNewConstMetric(
		c.counterDesc,
		prometheus.CounterValue,
		c.value,
		c.currentLabels...,
	)
}

func (c *muninCounter) Update(NewValue float64) {
	c.value = NewValue
}
func (c *muninCounter) UpdateLabels(currentLabels []string, NewValue float64) {
	c.value = NewValue
	c.currentLabels = currentLabels
}

func newMuninCounter(metricName string, desc string, VariableLabels []string, constlabels prometheus.Labels) *muninCounter {
	return &muninCounter{
		counterDesc: prometheus.NewDesc(
			metricName,
			desc,
			[]string{VariableLabels[0], VariableLabels[1], VariableLabels[2]},
			constlabels,
		),
	}
}

func init() {
	flag.Parse()
	if (*version) {
		fmt.Println(version_string)
		os.Exit(1)
	}
	var err error
	gaugePerMetric = map[string]*prometheus.GaugeVec{}
	counterPerMetric = map[string]*muninCounter{}
	muninBanner = regexp.MustCompile(`# munin node at (.*)`)
	loggo.ConfigureLoggers(*logLevel)
	err = connect()
	if err != nil {
		rootLogger.Criticalf("Could not connect to %s: %s", *muninAddress, err)
		os.Exit(1)
	}
}

func serveStatus() {
	prom := prometheus.Handler()
	http.HandleFunc(*listeningPath, func(res http.ResponseWriter, req *http.Request){
		wg.Wait();
		prom.ServeHTTP(res, req)
	})
	if err := http.ListenAndServe(*listeningAddress, nil); err != nil {
		panic(err)
	}
}

func connect() (err error) {
	rootLogger.Infof("Connecting to %s", *muninAddress)
	globalConn, err = net.Dial(proto, *muninAddress)
	if err != nil {
		return
	}
	rootLogger.Debugf("connected!")

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
	rootLogger.Infof("Found hostname: %s", hostname)
	return
}

func muninCommand(cmd string) (reader *bufio.Reader, err error) {
	reader = bufio.NewReader(globalConn)

	fmt.Fprintf(globalConn, cmd+"\n")

	_, err = reader.Peek(1)
	switch err {
	case io.EOF:
		rootLogger.Infof("not connected anymore, closing connection")
		globalConn.Close()
		for {
			err = connect()
			if err == nil {
				break
			}
			rootLogger.Warningf("Couldn't reconnect: %s", err)
			time.Sleep(retryInterval * time.Second)
		}

		return muninCommand(cmd)
	case nil: //no error
		break
	default:
		rootLogger.Criticalf("Unexpected error: %s", err)
		os.Exit(1)
	}

	return
}

func muninList() (items []string, err error) {
	munin, err := muninCommand("list")
	if err != nil {
		rootLogger.Warningf("couldn't get list")
		return
	}

	response, err := munin.ReadString('\n') // we are only interested in the first line
	if err != nil {
		rootLogger.Warningf("couldn't read response")
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
		rootLogger.Warningf("couldn't get config for %s", name)
		return
	}

	for {
		line, err := resp.ReadString('\n')
		if err == io.EOF {
			rootLogger.Criticalf("unexpected EOF, retrying")
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

		keyParts := strings.Split(key, ".")
		if len(keyParts) > 1 { // it's a metric config (metric.label etc)
			if _, ok := config[keyParts[0]]; !ok { //FIXME: is there no better way?
				config[keyParts[0]] = make(map[string]string)
			}
			config[keyParts[0]][keyParts[1]] = value
		} else {
			graphConfig[keyParts[0]] = value
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
			metricName := strings.Replace(name+"_"+metric, "-", "_", -1)
			desc := graphConfig["graph_title"] + ": " + config["label"]
			if config["info"] != "" {
				desc = desc + ", " + config["info"]
			}
			muninType := strings.ToLower(config["type"])
			// muninType can be empty and defaults to gauge
			if muninType == "counter" || muninType == "derive" {
				gv := newMuninCounter(metricName, desc, []string{"hostname", "graphname", "muninlabel"}, prometheus.Labels{"type": muninType})
				rootLogger.Infof("Registered counter %s: %s", metricName, desc)
				counterPerMetric[metricName] = gv
				prometheus.Register(gv)

			} else {
				gv := prometheus.NewGaugeVec(
					prometheus.GaugeOpts{
						Name:        metricName,
						Help:        desc,
						ConstLabels: prometheus.Labels{"type": "gauge"},
					},
					[]string{"hostname", "graphname", "muninlabel"},
				)
				rootLogger.Infof("Registered gauge %s: %s", metricName, desc)
				gaugePerMetric[metricName] = gv
				prometheus.Register(gv)
			}
		}
	}
	version_metric := prometheus.NewGaugeVec(
                prometheus.GaugeOpts{
                        Name:      "munin_exporter_build_info",
                        Help: fmt.Sprintf(
                                "A metric with a constant '1' value labeled by version, revision, branch, and goversion from which %s was built.",
                                version_string,
                        ),
                },
                []string{"version", "goversion"},
        )
    version_metric.WithLabelValues(version_num, runtime.Version()).Set(1)
	prometheus.Register(version_metric)
	gv := prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:	"munin_exporter_munin_data_fetch_time",
				Help:	"A metric showing the amount of time it takes to get all the data from munin and it's plugins",
				ConstLabels: prometheus.Labels{"type": "gauge"},
			},
			[]string{"hostname"},
		)
	gaugePerMetric["munin_fetching_metric"] = gv
	prometheus.Register(gv)
	return nil
}

func fetchMetrics() (err error) {
	wg.Add(1)
	start := time.Now()
	for _, graph := range graphs {
		munin, err := muninCommand("fetch " + graph)
		if err != nil {
			return err
		}

		for {
			line, err := munin.ReadString('\n')
			line = strings.TrimRight(line, "\n")
			if err == io.EOF {
				rootLogger.Criticalf("unexpected EOF, retrying")
				return fetchMetrics()
			}
			if err != nil {
				return err
			}
			if len(line) == 1 && line[0] == '.' {
				rootLogger.Debugf("End of list")

				break
			}

			parts := strings.Fields(line)
			if len(parts) != 2 {
				rootLogger.Debugf("unexpected line: %s", line)
				continue
			}
			key, valueString := strings.Split(parts[0], ".")[0], parts[1]
			value, err := strconv.ParseFloat(valueString, 64)
			if err != nil {
				rootLogger.Warningf("Couldn't parse value in line %s, malformed?", line)
				continue
			}
			name := strings.Replace(graph+"_"+key, "-", "_", -1)
			_, isGauge := gaugePerMetric[name]
			if isGauge {
				gaugePerMetric[name].WithLabelValues(hostname, graph, key).Set(value)
				rootLogger.Debugf("Gauge %s: %f\n", name, value)
				continue
			}
			_, isCounter := counterPerMetric[name]
			if isCounter {
				rootLogger.Debugf("Counter %s: %f\n", name, value)
				counterPerMetric[name].UpdateLabels([]string{hostname, graph, key}, value)
				continue
			}
		}
	}
	gaugePerMetric["munin_fetching_metric"].WithLabelValues(hostname).Set(time.Since(start).Seconds())
	wg.Done()
	return
}

func main() {
	flag.Parse()
	err := registerMetrics()
	if err != nil {
		rootLogger.Criticalf("Could not register metrics: %s", err)
		os.Exit(1)
	}

	go serveStatus()

	func() {
		ticker := time.NewTicker(time.Duration(*muninScrapeInterval)*time.Second)
		for range ticker.C {
			rootLogger.Debugf("Scrapping")
			err := fetchMetrics()
			if err != nil {
				rootLogger.Warningf("Error occured when trying to fetch metrics: %s", err)
			}
		}
	}()
}
