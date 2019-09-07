package gogroongahttpd

import (
	"fmt"
	"io"
	"net/http"
)

type Groonga struct {
	Scheme string
	Host   string
	Port   string
}

func (g *Groonga) createHTTPRequest(method, requestURL string, content io.Reader) (*http.Request, error) {
	return http.NewRequest(method, requestURL, content)
}

func (g *Groonga) createURLString(cmd, param string) string {
	return fmt.Sprintf("%s://%s:%s/d/%s?%s", g.Scheme, g.Host, g.Port, cmd, param)
}

func (g *Groonga) Select(param string) (*http.Response, error) {
	requestURL := g.createURLString("select", param)
	req, err := g.createHTTPRequest("GET", requestURL, nil)
	if err != nil {
		return nil, err
	}

	var c http.Client
	return c.Do(req)
}

func (g *Groonga) Load(param string, content io.Reader) (*http.Response, error) {
	requestURL := g.createURLString("load", param)
	req, err := g.createHTTPRequest("POST", requestURL, content)
	if err != nil {
		return nil, err
	}

	var c http.Client
	return c.Do(req)
}

func (g *Groonga) Delete(param string) (*http.Response, error) {
	requestURL := g.createURLString("delete", param)
	req, err := g.createHTTPRequest("GET", requestURL, nil)
	if err != nil {
		return nil, err
	}

	var c http.Client
	return c.Do(req)
}

func (g *Groonga) Status() (*http.Response, error) {
	requestURL := g.createURLString("status", "")
	req, err := g.createHTTPRequest("GET", requestURL, nil)
	if err != nil {
		return nil, err
	}

	var c http.Client
	return c.Do(req)
}
