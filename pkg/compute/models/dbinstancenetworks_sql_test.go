package models

import (
	"strings"
	"testing"

	"yunion.io/x/sqlchemy"
	_ "yunion.io/x/sqlchemy/backends/mysql"
)

func TestDBNetworkSubnetOnlySQLPreservesEmptyString(t *testing.T) {
	const database sqlchemy.DBName = "dbnetwork_sql_regression"
	sqlchemy.SetDBWithNameBackend(nil, database, sqlchemy.MySQLBackend)
	table := sqlchemy.NewTableSpecFromStructWithDBName(SDBInstanceNetwork{}, "dbinstancenetworks_tbl", database)
	row := &SDBInstanceNetwork{NetworkId: "subnet-id"}
	row.DBInstanceId = "db-id"
	statement, err := table.InsertSqlPrep(row, false)
	if err != nil {
		t.Fatal(err)
	}
	// Inspect actual prepared SQL and parameter position, not a stand-in struct.
	left := strings.Index(statement.Sql, "(")
	right := strings.Index(statement.Sql, ")")
	if left < 0 || right < left {
		t.Fatalf("unexpected SQL: %s", statement.Sql)
	}
	columns := strings.Split(statement.Sql[left+1:right], ",")
	valueStart := strings.Index(statement.Sql, "VALUES")
	if valueStart < 0 {
		t.Fatalf("missing VALUES: %s", statement.Sql)
	}
	valueSql := statement.Sql[valueStart:]
	formats := strings.Split(valueSql[strings.Index(valueSql, "(")+1:strings.LastIndex(valueSql, ")")], ",")
	paramIndex := 0
	found := false
	for i, name := range columns {
		if strings.Trim(strings.TrimSpace(name), "`") == "ip_addr" {
			found = true
			if strings.TrimSpace(formats[i]) != "?" {
				t.Fatalf("unknown IP must use bound empty string, format=%q SQL=%s values=%#v", formats[i], statement.Sql, statement.Values)
			}
			value, ok := statement.Values[paramIndex].(string)
			if !ok || value != "" {
				t.Fatalf("unknown IP must bind empty VARCHAR string, got %#v; SQL=%s", statement.Values[paramIndex], statement.Sql)
			}
		}
		paramIndex += strings.Count(formats[i], "?")
	}
	if !found {
		t.Fatalf("IP column missing from prepared SQL: %s", statement.Sql)
	}
	for _, column := range table.Columns() {
		if column.Name() == "ip_addr" && column.IsNullable() {
			t.Fatal("empty VARCHAR is supported; retain NOT NULL for uniqueness")
		}
	}
}
