package main

import (
	"io"
	"net/http"
	"time"

	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"gopkg.in/tylerb/graceful.v1"
)

type p2cRequest struct {
	name string
	tags []string
	val  float64
	ts   time.Time
}

type p2cServer struct {
	requests chan *p2cRequest
	mux      *http.ServeMux
	conf     *config
	writer   *p2cWriter
	reader   *p2cReader
	rx       prometheus.Counter
}

func NewP2CServer(conf *config) (*p2cServer, error) {
	var err error
	c := new(p2cServer)
	c.requests = make(chan *p2cRequest, conf.ChanSize)
	c.mux = http.NewServeMux()
	c.conf = conf

	c.writer, err = NewP2CWriter(conf, c.requests)
	if err != nil {
		fmt.Printf("Error creating clickhouse writer: %s\n", err.Error())
		return c, err
	}

	c.reader, err = NewP2CReader(conf)
	if err != nil {
		fmt.Printf("Error creating clickhouse reader: %s\n", err.Error())
		return c, err
	}

	c.rx = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "received_samples_total",
			Help: "Total number of received samples.",
		},
	)
	prometheus.MustRegister(c.rx)

	c.mux.HandleFunc(c.conf.HTTPWritePath, func(w http.ResponseWriter, r *http.Request) {
		// modify by jiangkun0928 for 升级clickhouse client版本到v2.17.1 on 20240131 start
		// compressed, err := ioutil.ReadAll(r.Body)
		compressed, err := io.ReadAll(r.Body)
		// modify by jiangkun0928 for 升级clickhouse client版本到v2.17.1 on 20240131 end
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// modify by jiangkun0928 for 功能优化 on 20240130 start
		// var req remote.WriteRequest
		var req prompb.WriteRequest
		// modify by jiangkun0928 for 功能优化 on 20240130 end
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		c.process(req)
	})
	// modify by jiangkun0928 for 升级clickhouse client版本到v2.17.1 on 20240131 start
	// c.mux.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
	// compressed, err := ioutil.ReadAll(r.Body)
	c.mux.HandleFunc(c.conf.HTTPReadPath, func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		// modify by jiangkun0928 for 升级clickhouse client版本到v2.17.1 on 20240131 end
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// modify by jiangkun0928 for 功能优化 on 20240130 start
		// var req remote.ReadRequest
		var req prompb.ReadRequest
		// modify by jiangkun0928 for 功能优化 on 20240130 end
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// modify by jiangkun0928 for 功能优化 on 20240130 start
		// var resp *remote.ReadResponse
		var resp *prompb.ReadResponse
		// modify by jiangkun0928 for 功能优化 on 20240130 end
		resp, err = c.reader.Read(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	// modify by jiangkun0928 for 功能优化 on 20240130 start
	// c.mux.Handle(c.conf.HTTPMetricsPath, prometheus.InstrumentHandler(
	// 	c.conf.HTTPMetricsPath, prometheus.UninstrumentedHandler(),
	// ))
	c.mux.Handle(c.conf.HTTPMetricsPath, promhttp.Handler())
	// modify by jiangkun0928 for 功能优化 on 20240130 end

	return c, nil
}

// modify by jiangkun0928 for 功能优化 on 20240130 start
// func (c *p2cServer) process(req remote.WriteRequest) {
func (c *p2cServer) process(req prompb.WriteRequest) {
	// modify by jiangkun0928 for 功能优化 on 20240130 end
	for _, series := range req.Timeseries {
		c.rx.Add(float64(len(series.Samples)))
		var (
			name string
			tags []string
		)

		for _, label := range series.Labels {
			if model.LabelName(label.Name) == model.MetricNameLabel {
				name = label.Value
			}
			// store tags in <key>=<value> format
			// allows for has(tags, "key=val") searches
			// probably impossible/difficult to do regex searches on tags
			t := fmt.Sprintf("%s=%s", label.Name, label.Value)
			tags = append(tags, t)
		}

		for _, sample := range series.Samples {
			p2c := new(p2cRequest)
			p2c.name = name
			// modify by jiangkun0928 for 功能优化 on 20240130 start
			// p2c.ts = time.Unix(sample.TimestampMs/1000, 0)
			p2c.ts = time.Unix(sample.Timestamp/1000, 0)
			// modify by jiangkun0928 for 功能优化 on 20240130 end
			p2c.val = sample.Value
			p2c.tags = tags
			c.requests <- p2c
		}

	}
}

func (c *p2cServer) Start() error {
	fmt.Println("HTTP server starting...")
	c.writer.Start()
	return graceful.RunWithErr(c.conf.HTTPAddr, c.conf.HTTPTimeout, c.mux)
}

func (c *p2cServer) Shutdown() {
	close(c.requests)
	c.writer.Wait()

	wchan := make(chan struct{})
	go func() {
		c.writer.Wait()
		close(wchan)
	}()

	select {
	case <-wchan:
		fmt.Println("Writer shutdown cleanly..")
	// All done!
	case <-time.After(10 * time.Second):
		fmt.Println("Writer shutdown timed out, samples will be lost..")
	}

}
