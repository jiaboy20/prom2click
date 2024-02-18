package main

import (
	"database/sql"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func GetOpenDBConnection(addr []string, database string, username string, password string) (*sql.DB, error) {
	settings := clickhouse.Settings{}
	settings["wait_end_of_query"] = 1
	settings["insert_quorum"] = 1
	settings["insert_quorum_parallel"] = 0
	settings["select_sequential_consistency"] = 1
	settings["database_replicated_enforce_synchronous_settings"] = "1"
	return clickhouse.OpenDB(&clickhouse.Options{
		Addr: addr,
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: settings,
		Protocol: clickhouse.HTTP,
	}), nil
}
