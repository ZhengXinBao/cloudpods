package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"yunion.io/x/sqlchemy"
	_ "yunion.io/x/sqlchemy/backends/mysql"
)

// A database/sql boundary double exercises the actual SQL reader, including
// errors delivered by the cursor only after the expected rows were received.
type inventorySQLConnector struct {
	failure error
	query   string
	args    []driver.NamedValue
}

func (c *inventorySQLConnector) Connect(context.Context) (driver.Conn, error) {
	return &inventorySQLConn{c}, nil
}
func (c *inventorySQLConnector) Driver() driver.Driver { return inventorySQLDriver{} }

type inventorySQLDriver struct{}

func (inventorySQLDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type inventorySQLConn struct{ c *inventorySQLConnector }

func (c *inventorySQLConn) Close() error { return nil }
func (c *inventorySQLConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}
func (c *inventorySQLConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c *inventorySQLConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.c.query = query
	c.c.args = args
	return &inventorySQLRows{failure: c.c.failure}, nil
}

type inventorySQLRows struct {
	read    bool
	failure error
}

func (r *inventorySQLRows) Columns() []string { return []string{"external_id"} }
func (r *inventorySQLRows) Close() error      { return nil }
func (r *inventorySQLRows) Next(dest []driver.Value) error {
	if !r.read {
		r.read = true
		dest[0] = "lb-a"
		return nil
	}
	if r.failure != nil {
		return r.failure
	}
	return io.EOF
}

func TestCloudInventorySQLCursorFailureIsNotSuccess(t *testing.T) {
	for _, fail := range []bool{false, true} {
		connector := &inventorySQLConnector{}
		if fail {
			connector.failure = errors.New("inventory cursor interrupted")
		}
		conn := sql.OpenDB(connector)
		defer conn.Close()
		database := sqlchemy.DBName("inventory_sql_success")
		if fail {
			database = "inventory_sql_cursor_failure"
		}
		sqlchemy.SetDBWithNameBackend(conn, database, sqlchemy.MySQLBackend)
		table := sqlchemy.NewTableSpecFromStructWithDBName(struct {
			ExternalId    string
			ManagerId     string
			CloudregionId string
			Secret        string
		}{}, "inventory_test_tbl", database)
		q := table.Query().Equals("manager_id", "provider-a").Equals("cloudregion_id", "region-shanghai")
		err := verifyCloudInventoryQuery([]string{"lb-a"}, q)
		if fail && !errors.Is(err, connector.failure) {
			t.Fatalf("cursor error was concealed by complete first row: %v", err)
		}
		if !fail && err != nil {
			t.Fatal(err)
		}
		selectSQL := strings.Split(connector.query, "FROM")[0]
		if strings.Contains(strings.ToLower(selectSQL), "secret") || !strings.Contains(selectSQL, "external_id") {
			t.Fatalf("audit selected more than identity: %s", connector.query)
		}
		if len(connector.args) != 2 || connector.args[0].Value != "provider-a" || connector.args[1].Value != "region-shanghai" {
			t.Fatalf("lost regional/provider query scope: %s %v", connector.query, connector.args)
		}
	}
}
