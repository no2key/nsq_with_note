package util

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

type HTTPError struct {
	Code int
	Text string
}

func (e HTTPError) Error() string {
	return e.Text
}

func acceptVersion(req *http.Request) int {
	if req.Header.Get("accept") == "application/vnd.nsq; version=1.0" {
		return 1
	}

	return 0
}

// 装饰器：必须使用POST
// 参数传入（req和一个（返回数据和错误的）函数），返回一个（返回数据和错误的）函数。
// 实质是根据req返回原有函数，或返回405错误函数。
func POSTRequired(req *http.Request, f func() (interface{}, error)) func() (interface{}, error) {
	if req.Method != "POST" {
		return func() (interface{}, error) {
			return nil, HTTPError{405, "INVALID_REQUEST"}
		}
	}
	return f
}

// 包装器：传入req和response和处理二者关系用的func，先用func处理req，根据处理后req的特征决定返回何种response（V1Api或Api）
func NegotiateAPIResponseWrapper(w http.ResponseWriter, req *http.Request, f func() (interface{}, error)) {
	data, err := f()
	if err != nil {
		if acceptVersion(req) == 1 {
			V1ApiResponse(w, err.(HTTPError).Code, err)
		} else {
			// this handler always returns 500 for backwards compatibility
			ApiResponse(w, 500, err.Error(), nil)
		}
		return
	}
	if acceptVersion(req) == 1 {
		V1ApiResponse(w, 200, data)
	} else {
		ApiResponse(w, 200, "OK", data)
	}
}

// 包装器：返回V1Api格式的response
func V1APIResponseWrapper(w http.ResponseWriter, req *http.Request, f func() (interface{}, error)) {
	data, err := f()
	if err != nil {
		V1ApiResponse(w, err.(HTTPError).Code, err)
		return
	}
	V1ApiResponse(w, 200, data)
}


// API响应格式
func ApiResponse(w http.ResponseWriter, statusCode int, statusTxt string, data interface{}) {
	var response []byte
	var err error

	switch data.(type) {
	case string:
		response = []byte(data.(string))
	case []byte:
		response = data.([]byte)
	default:
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		response, err = json.Marshal(struct {
			StatusCode int         `json:"status_code"`
			StatusTxt  string      `json:"status_txt"`
			Data       interface{} `json:"data"`
		}{
			statusCode,
			statusTxt,
			data,
		})
		if err != nil {
			response = []byte(fmt.Sprintf(`{"status_code":500, "status_txt":"%s", "data":null}`, err))
		}
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.WriteHeader(statusCode)
	w.Write(response)
}

// V1Api响应格式
func V1ApiResponse(w http.ResponseWriter, code int, data interface{}) {
	var response []byte
	var err error
	var isJson bool

	if code == 200 {
		switch data.(type) {
		case string:
			response = []byte(data.(string))
		case []byte:
			response = data.([]byte)
		case nil:
			response = []byte{}
		default:
			isJson = true
			response, err = json.Marshal(data)
			if err != nil {
				code = 500
				data = err
			}
		}
	}

	if code != 200 {
		isJson = true
		response = []byte(fmt.Sprintf(`{"message":"%s"}`, data))
	}

	if isJson {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}
	w.Header().Set("X-NSQ-Content-Type", "nsq; version=1.0")
	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.WriteHeader(code)
	w.Write(response)
}
