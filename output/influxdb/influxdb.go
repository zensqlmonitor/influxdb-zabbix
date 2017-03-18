package influxdb

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
)

type Loader interface {
	Load() error
}

type loader struct {
	url        string
	database   string
	username   string
	password   string
	precision  string
	inlinedata string
}

var _ Loader = (*loader)(nil)

func NewLoader(url, user, pass, inlinedata string) loader {
	loa := loader{}
	loa.url = url
        loa.username = user
        loa.password = pass
	loa.inlinedata = inlinedata
	return loa
}

// 2xx: If it's HTTP 204 No Content, success!
//      If it's HTTP 200 OK, InfluxDB understood the request but couldn't complete it.
// 4xx: InfluxDB could not understand the request.
// 5xx: The system is overloaded or significantly impaired
func (loa *loader) Load() error {

	client := &http.Client{}
	req, err := http.NewRequest("POST", loa.url, bytes.NewBufferString(loa.inlinedata))
	req.Header.Set("Content-Type", "application/text")
	if len(loa.username) > 0 {
		req.SetBasicAuth(loa.username, loa.password)
	}
	params := req.URL.Query()
	if len(loa.precision) > 0 {
		params.Set("precision", loa.precision)
	}
	req.URL.RawQuery = params.Encode()
	resp, err := client.Do(req)

	if err != nil {
		// Handle error
		return errors.New(err.Error())
	}
	defer resp.Body.Close()

	// read response
	htmlData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// Handle error
		return errors.New(err.Error())
	}
	// check for success
	if !strings.Contains(resp.Status, "204") {
		// Handle error
		return errors.New(string(htmlData))
	}
	return nil

}
