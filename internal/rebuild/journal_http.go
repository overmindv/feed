package rebuild

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type journalHTTP struct {
	endpoint url.URL
	token    string
	client   *http.Client
}

type journalStatusError int

func (e journalStatusError) Error() string {
	return fmt.Sprintf("journal returned HTTP %d", int(e))
}

func newJournalHTTP(address, token, path, urlEnv, tokenEnv string, allowHTTP bool) (*journalHTTP, error) {
	endpoint, err := url.Parse(address)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" ||
		endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.RawPath != "" ||
		(endpoint.Path != "" && endpoint.Path != "/") ||
		(endpoint.Scheme != "https" && (!allowHTTP || endpoint.Scheme != "http")) {
		return nil, fmt.Errorf("%s must be a base URL without credentials, query or path", urlEnv)
	}
	if !validJournalToken(token) {
		return nil, fmt.Errorf("%s must contain 32-512 printable ASCII characters without spaces", tokenEnv)
	}
	endpoint.Path = path

	return &journalHTTP{
		endpoint: *endpoint, token: token,
		client: &http.Client{
			Transport:     &http.Transport{MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second},
			Timeout:       10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

func validJournalToken(token string) bool {
	return len(token) >= 32 && len(token) <= 512 &&
		!strings.ContainsFunc(token, func(r rune) bool { return r < 0x21 || r > 0x7e })
}

func (j *journalHTTP) CloseIdleConnections() {
	j.client.CloseIdleConnections()
}

func (j *journalHTTP) readPage(ctx context.Context, watermark *int64, after int64, limit, maxBytes int) ([]byte, error) {
	target := j.endpoint
	query := url.Values{"after": {strconv.FormatInt(after, 10)}, "limit": {strconv.Itoa(limit)}}
	if watermark != nil {
		query.Set("watermark", strconv.FormatInt(*watermark, 10))
	}
	target.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, errors.New("cannot build journal request")
	}
	request.Header.Set("Authorization", "Bearer "+j.token)
	request.Header.Set("Accept", "application/json")
	response, err := j.client.Do(request)
	if err != nil {
		return nil, errors.New("cannot reach journal")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, journalStatusError(response.StatusCode)
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, int64(maxBytes)+1))
	if err != nil || len(payload) > maxBytes {
		return nil, errors.New("cannot read bounded journal response")
	}

	return payload, nil
}
