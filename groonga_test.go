package gogroongahttpd

import (
	"bytes"
	"flag"
	"log"
	"testing"
)

var (
	scheme, host, port string
)

func preprocess() {
	g := Groonga{
		Scheme: scheme,
		Host:   host,
		Port:   port,
	}

	_, err := g.CreateTable("name=Site&key_type=Int32")
	if err != nil {
		log.Fatal(err)
	}

	_, err = g.CreateColumn("table=Site&name=name&flags=COLUMN_VECTOR&type=Text")
	if err != nil {
		log.Fatal(err)
	}

	_, err = g.CreateColumn("table=Site&name=age&flags=COLUMN_VECTOR&type=Int8")
	if err != nil {
		log.Fatal(err)
	}

	data := `
        [
            {"_key":1,"name":"aaa","age":20},
            {"_key":2,"name":"bbb","age":30},
            {"_key":3,"name":"ccc","age":15}
        ]
    `

	_, err = g.Load("table=Site", bytes.NewBufferString(data))
	if err != nil {
		log.Fatal(err)
	}
}

func postprocess() {
	g := Groonga{
		Scheme: scheme,
		Host:   host,
		Port:   port,
	}

	_, err := g.DeleteTable("name=Site")
	if err != nil {
		log.Fatal(err)
	}
}

func TestMain(m *testing.M) {
	flag.StringVar(&scheme, "scheme", "http", "Specify scheme")
	flag.StringVar(&host, "host", "192.168.56.11", "Specify host")
	flag.StringVar(&port, "port", "10041", "Specify port")
	flag.Parse()

	preprocess()

	defer postprocess()

	m.Run()
}

func TestCreateTable(t *testing.T) {
	g := Groonga{
		Scheme: scheme,
		Host:   host,
		Port:   port,
	}
	res, err := g.CreateTable("name=TestTable&key_type=Int8")
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != 200 {
		t.Errorf("Could not match StatusCode\nexpect: %d\nactual: %d\n", 200, res.StatusCode)
	}
}

func TestCreateColumn(t *testing.T) {
	g := Groonga{
		Scheme: scheme,
		Host:   host,
		Port:   port,
	}
	res, err := g.CreateColumn("table=TestTable&name=name&flags=COLUMN_VECTOR&type=Text")
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != 200 {
		t.Errorf("Could not match StatusCode\nexpect: %d\nactual: %d\n", 200, res.StatusCode)
	}
}

func TestDeleteTable(t *testing.T) {
	g := Groonga{
		Scheme: scheme,
		Host:   host,
		Port:   port,
	}
	res, err := g.DeleteTable("name=TestTable")
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != 200 {
		t.Errorf("Could not match StatusCode\nexpect: %d\nactual: %d\n", 200, res.StatusCode)
	}
}

func TestSelect(t *testing.T) {
	g := Groonga{
		Scheme: scheme,
		Host:   host,
		Port:   port,
	}
	res, err := g.Select("table=Site&_id==1")
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != 200 {
		t.Errorf("Could not match StatusCode\nexpect: %d\nactual: %d\n", 200, res.StatusCode)
	}
}

func TestLoad(t *testing.T) {
	g := Groonga{
		Scheme: scheme,
		Host:   host,
		Port:   port,
	}

	buf := bytes.NewBufferString(`{"_key":4,"name":"ddd","age":40}`)
	res, err := g.Load("table=Site", buf)
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != 200 {
		t.Errorf("Could not match StatusCode\nexpect: %d\nactual: %d\n", 200, res.StatusCode)
	}
}

func TestDelete(t *testing.T) {
	g := Groonga{
		Scheme: scheme,
		Host:   host,
		Port:   port,
	}

	res, err := g.Delete(`table=Site&filter=_key==4`)
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != 200 {
		t.Errorf("Could not match StatusCode\nexpect: %d\nactual: %d\n", 200, res.StatusCode)
	}
}

func TestStatus(t *testing.T) {
	g := Groonga{
		Scheme: scheme,
		Host:   host,
		Port:   port,
	}

	res, err := g.Status()
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != 200 {
		t.Errorf("Could not match StatusCode\nexpect: %d\nactual: %d\n", 200, res.StatusCode)
	}
}
