package groonga

import (
	"bytes"
	"flag"
	"log"
	"os"
	"testing"
)

var (
	scheme, host, port string
)

func TestMain(m *testing.M) {
	flag.StringVar(&scheme, "scheme", "http", "Specify scheme")
	flag.StringVar(&host, "host", "192.168.56.11", "Specify host")
	flag.StringVar(&port, "port", "10041", "Specify port")
	flag.Parse()

	os.Exit(m.Run())
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

	log.Println(res)
}

func TestLoad(t *testing.T) {
	g := Groonga{
		Scheme: scheme,
		Host:   host,
		Port:   port,
	}

	buf := bytes.NewBufferString(`{"_key":"nice","title":"good"}`)
	res, err := g.Load("table=Site", buf)
	if err != nil {
		t.Fatal(err)
	}

	log.Println(res)
}

func TestDelete(t *testing.T) {
	g := Groonga{
		Scheme: scheme,
		Host:   host,
		Port:   port,
	}

	res, err := g.Delete(`table=Site&filter=_key=="nice"`)
	if err != nil {
		t.Fatal(err)
	}

	log.Println(res)
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

	log.Println(res)
}
