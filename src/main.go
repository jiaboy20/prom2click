package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// a lot of this borrows directly from:
// 	https://github.com/prometheus/prometheus/blob/master/documentation/examples/remote_storage/remote_storage_adapter/main.go

type config struct {
	//tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
	// modify by jiangkun0928 for 升级clickhouse client版本到v2.17.1 on 20240130 start
	// ChDSN    string
	ChAddr   []string
	ChUser   string
	ChPasswd string
	// modify by jiangkun0928 for 升级clickhouse client版本到v2.17.1 on 20240130 end
	ChDB string
	// modify by jiangkun0928 for 功能优化 on 20240130 start
	// ChTable  string
	ChWriteTable string
	ChReadTable  string
	// modify by jiangkun0928 for 功能优化 on 20240130 end
	ChBatch  int
	ChanSize int
	// delete by jiangkun0928 for 采用clickhouse的graphite_rollup实现数据上卷，删除无用的配置项 on 20240130 start
	// CHQuantile      float64
	// CHMaxSamples    int
	// CHMinPeriod     int
	// delete by jiangkun0928 for 采用clickhouse的graphite_rollup实现数据上卷，删除无用的配置项 on 20240130 end
	HTTPTimeout   time.Duration
	HTTPAddr      string
	HTTPWritePath string
	// add by jiangkun0928 for 功能优化 on 20240130 start
	HTTPReadPath string
	// add by jiangkun0928 for 功能优化 on 20240130 end
	HTTPMetricsPath string
}

var (
	versionFlag bool
)

func main() {
	excode := 0

	conf := parseFlags()

	if versionFlag {
		fmt.Println("Git Commit:", GitCommit)
		fmt.Println("Version:", Version)
		if VersionPrerelease != "" {
			fmt.Println("Version PreRelease:", VersionPrerelease)
		}
		os.Exit(excode)
	}

	fmt.Println("Starting up..")

	srv, err := NewP2CServer(conf)
	if err != nil {
		fmt.Printf("Error: could not create server: %s\n", err.Error())
		excode = 1
		os.Exit(excode)
	}

	err = srv.Start()
	if err != nil {
		fmt.Printf("Error: http server returned error: %s\n", err.Error())
		excode = 1
	}

	fmt.Println("Shutting down..")
	srv.Shutdown()
	fmt.Println("Exiting..")
	os.Exit(excode)
}

func parseFlags() *config {
	cfg := new(config)

	// print version?
	flag.BoolVar(&versionFlag, "version", false, "Version")
	// modify by jiangkun0928 for 升级clickhouse client版本到v2.17.1 on 20240130 start
	// // clickhouse dsn
	// ddsn := "tcp://127.0.0.1:9000?username=&password=&database=metrics&" +
	// 	"read_timeout=10&write_timeout=10&alt_hosts="
	// flag.StringVar(&cfg.ChDSN, "ch.dsn", ddsn,
	// 	"The clickhouse server DSN to write to eg."+
	// 		"tcp://host1:9000?username=user&password=qwerty&database=clicks&"+
	// 		"read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000"+
	// 		"(see https://github.com/kshvakov/clickhouse).",
	// )
	var tmpChAddr string
	flag.StringVar(&tmpChAddr, "ch.addr", "127.0.0.1:8123",
		"The clickhouse server host:port eg.127.0.0.1:8123,127.0.0.1:8123,127.0.0.1:8123 (see https://github.com/Clickhouse/clickhouse-go).",
	)

	flag.StringVar(&cfg.ChUser, "ch.user", "abc",
		"The clickhouse account username (see https://github.com/Clickhouse/clickhouse-go).",
	)

	flag.StringVar(&cfg.ChPasswd, "ch.password", "abc@1234",
		"The clickhouse account password (see https://github.com/Clickhouse/clickhouse-go).",
	)

	// modify by jiangkun0928 for 升级clickhouse client版本到v2.17.1 on 20240130 end
	// clickhouse db
	flag.StringVar(&cfg.ChDB, "ch.db", "metrics",
		"The clickhouse database to write to.",
	)
	// modify by jiangkun0928 for 功能优化 on 20240130 start
	// clickhouse table
	// flag.StringVar(&cfg.ChTable, "ch.table", "samples",
	// 	"The clickhouse table to write to.",
	// )
	flag.StringVar(&cfg.ChWriteTable, "ch.writeTable", "samples",
		"The clickhouse table to write to.",
	)
	flag.StringVar(&cfg.ChReadTable, "ch.readTable", "",
		"The clickhouse table to read from. If not set, use ch.writeTable to read from.",
	)
	// modify by jiangkun0928 for 功能优化 on 20240130 end
	// clickhouse insertion batch size
	flag.IntVar(&cfg.ChBatch, "ch.batch", 50000,
		"Clickhouse write batch size (n metrics).",
	)

	// channel buffer size between http server => clickhouse writer(s)
	flag.IntVar(&cfg.ChanSize, "ch.buffer", 50000,
		"Maximum internal channel buffer size (n requests).",
	)
	// delete by jiangkun0928 for 采用clickhouse的graphite_rollup实现数据上卷，删除无用的配置项 on 20240130 start
	// // quantile (eg. 0.9 for 90th) for aggregation of timeseries values from CH
	// flag.Float64Var(&cfg.CHQuantile, "ch.quantile", 0.75,
	// 	"Quantile/Percentile for time series aggregation when the number "+
	// 		"of points exceeds ch.maxsamples.",
	// )

	// // maximum number of samples to return
	// // todo: fixup strings.. yuck.
	// flag.IntVar(&cfg.CHMaxSamples, "ch.maxsamples", 8192,
	// 	"Maximum number of samples to return to Prometheus for a remote read "+
	// 		"request - the minimum accepted value is 50. "+
	// 		"Note: if you set this too low there can be issues displaying graphs in grafana. "+
	// 		"Increasing this will cause query times and memory utilization to grow. You'll "+
	// 		"probably need to experiment with this.",
	// )
	// // need to ensure this isn't 0 - divide by 0..
	// if cfg.CHMaxSamples < 50 {
	// 	fmt.Printf("Error: invalid ch.maxsamples of %d - minimum is 50\n", cfg.CHMaxSamples)
	// 	os.Exit(1)
	// }

	// // http shutdown and request timeout
	// flag.IntVar(&cfg.CHMinPeriod, "ch.minperiod", 10,
	// 	"The minimum time range for Clickhouse time aggregation in seconds.",
	// )
	// delete by jiangkun0928 for 采用clickhouse的graphite_rollup实现数据上卷，删除无用的配置项 on 20240130 end

	// http listen address
	flag.StringVar(&cfg.HTTPAddr, "web.address", ":9201",
		"Address to listen on for web endpoints.",
	)

	// http prometheus remote write endpoint
	flag.StringVar(&cfg.HTTPWritePath, "web.write", "/api/write",
		"Address to listen on for remote write requests.",
	)
	// add by jiangkun0928 for 功能优化 on 20240130 start
	flag.StringVar(&cfg.HTTPReadPath, "web.read", "/api/read",
		"Address to listen on for remote read requests.",
	)
	// add by jiangkun0928 for 功能优化 on 20240130 end
	// http prometheus metrics endpoint
	flag.StringVar(&cfg.HTTPMetricsPath, "web.metrics", "/metrics",
		"Address to listen on for metric requests.",
	)

	// http shutdown and request timeout
	flag.DurationVar(&cfg.HTTPTimeout, "web.timeout", 30*time.Second,
		"The timeout to use for HTTP requests and server shutdown. Defaults to 30s.",
	)

	flag.Parse()
	// add by jiangkun0928 for 功能优化 on 20240130 start
	cfg.ChAddr = strings.Split(tmpChAddr, ",")
	if cfg.ChReadTable == "" {
		cfg.ChReadTable = cfg.ChWriteTable
	}
	// add by jiangkun0928 for 功能优化 on 20240130 end
	return cfg
}
