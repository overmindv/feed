package rebuild

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	taskPageSize    = 100
	maxTaskPage     = 512 << 10
	MaxTaskPages    = 200
	taskJournalPath = "/v1/internal/task-activity/changes"

	TaskPublished   = "task.published"
	TaskUnpublished = "task.unpublished"
)

var (
	uuidPattern = regexp.MustCompile("(?i)^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
	runPattern  = regexp.MustCompile("^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

	ErrTaskJournalGap = errors.New("invalid or incomplete Tasks journal page")
)

// TaskChange contains only the public fields of a Tasks journal entry.
type TaskChange struct {
	Revision      int64     `json:"revision"`
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	OccurredAt    time.Time `json:"occurred_at"`
	TaskID        string    `json:"task_id"`
	TaskVersionID string    `json:"task_version_id,omitempty"`
	Title         string    `json:"title,omitempty"`
	TopicID       string    `json:"topic_id,omitempty"`
	TaskType      string    `json:"task_type,omitempty"`
	Difficulty    string    `json:"difficulty,omitempty"`
}

type TaskCheckpoint struct {
	Watermark int64
	Through   int64
}

type TaskPage struct {
	TaskCheckpoint
	More    bool
	Changes []TaskChange
}

type TaskSource interface {
	ReadTaskPage(context.Context, *int64, int64) (TaskPage, error)
}

type TaskStager interface {
	LoadTaskCheckpoint(context.Context, string) (TaskCheckpoint, bool, error)
	SaveTaskPage(context.Context, string, int64, TaskPage) error
}

type TaskJournal struct {
	endpoint url.URL
	token    string
	client   *http.Client
}

func ValidRunID(id string) bool {
	return runPattern.MatchString(id)
}

func NewTaskJournal(address, token string, allowHTTP bool) (*TaskJournal, error) {
	endpoint, err := url.Parse(address)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" ||
		endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.RawPath != "" ||
		(endpoint.Path != "" && endpoint.Path != "/") ||
		(endpoint.Scheme != "https" && (!allowHTTP || endpoint.Scheme != "http")) {
		return nil, errors.New("FEED_TASKS_URL must be a Tasks base URL without credentials, query or path")
	}
	if len(token) < 32 || len(token) > 512 || strings.ContainsFunc(token, func(r rune) bool { return r < 0x21 || r > 0x7e }) {
		return nil, errors.New("TASKS_ACTIVITY_TOKEN must contain 32-512 printable ASCII characters without spaces")
	}
	endpoint.Path = taskJournalPath

	return &TaskJournal{
		endpoint: *endpoint, token: token,
		client: &http.Client{
			Transport:     &http.Transport{MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second},
			Timeout:       10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

func (j *TaskJournal) CloseIdleConnections() {
	j.client.CloseIdleConnections()
}

func (j *TaskJournal) ReadTaskPage(ctx context.Context, watermark *int64, after int64) (TaskPage, error) {
	target := j.endpoint
	query := url.Values{"after": {strconv.FormatInt(after, 10)}, "limit": {strconv.Itoa(taskPageSize)}}
	if watermark != nil {
		query.Set("watermark", strconv.FormatInt(*watermark, 10))
	}
	target.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return TaskPage{}, errors.New("cannot build Tasks journal request")
	}
	request.Header.Set("Authorization", "Bearer "+j.token)
	request.Header.Set("Accept", "application/json")
	response, err := j.client.Do(request)
	if err != nil {
		return TaskPage{}, errors.New("cannot reach Tasks journal")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return TaskPage{}, fmt.Errorf("tasks journal returned HTTP %d", response.StatusCode)
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxTaskPage+1))
	if err != nil || len(payload) > maxTaskPage {
		return TaskPage{}, errors.New("cannot read bounded Tasks journal response")
	}
	var wire struct {
		Watermark *int64       `json:"watermark"`
		Through   *int64       `json:"through"`
		More      *bool        `json:"more"`
		Changes   []TaskChange `json:"changes"`
	}
	if err = json.Unmarshal(payload, &wire); err != nil || wire.Watermark == nil || wire.Through == nil ||
		wire.More == nil || wire.Changes == nil {
		return TaskPage{}, ErrTaskJournalGap
	}

	return TaskPage{TaskCheckpoint: TaskCheckpoint{Watermark: *wire.Watermark, Through: *wire.Through},
		More: *wire.More, Changes: wire.Changes}, nil
}

func validID(value string) bool {
	return value != "00000000-0000-0000-0000-000000000000" && uuidPattern.MatchString(value)
}

func (c TaskChange) valid() bool {
	if !validID(c.EventID) || !validID(c.TaskID) || c.OccurredAt.IsZero() ||
		(c.TaskVersionID != "" && !validID(c.TaskVersionID)) || (c.TopicID != "" && !validID(c.TopicID)) ||
		strings.ContainsRune(c.Title, 0) || strings.ContainsRune(c.TaskType, 0) || strings.ContainsRune(c.Difficulty, 0) {
		return false
	}

	return c.EventType == TaskUnpublished ||
		(c.EventType == TaskPublished && c.TaskVersionID != "" && strings.TrimSpace(c.Title) != "")
}

func validTaskPage(page TaskPage, watermark *int64, after int64) bool {
	if after < 0 || (watermark != nil && page.Watermark != *watermark) ||
		page.Through < after || page.Watermark < page.Through || page.More != (page.Through < page.Watermark) ||
		len(page.Changes) > taskPageSize || page.Through-after != int64(len(page.Changes)) ||
		(page.More && len(page.Changes) == 0) {
		return false
	}
	for i, change := range page.Changes {
		if change.Revision != after+int64(i)+1 || !change.valid() {
			return false
		}
	}

	return true
}

// StageTasks resumes a shadow import without writing to the active feed or Kafka offsets.
func StageTasks(ctx context.Context, source TaskSource, stager TaskStager, id string, maxPages int) (TaskCheckpoint, error) {
	if !ValidRunID(id) || maxPages < 1 || maxPages > MaxTaskPages {
		return TaskCheckpoint{}, errors.New("invalid Tasks rebuild identifier or page budget")
	}
	checkpoint, found, err := stager.LoadTaskCheckpoint(ctx, id)
	if err != nil {
		return checkpoint, err
	}
	for range maxPages {
		var watermark *int64
		if found {
			if checkpoint.Through == checkpoint.Watermark {
				break
			}
			watermark = &checkpoint.Watermark
		}
		page, readErr := source.ReadTaskPage(ctx, watermark, checkpoint.Through)
		if readErr != nil {
			return checkpoint, readErr
		}
		if !validTaskPage(page, watermark, checkpoint.Through) {
			return checkpoint, ErrTaskJournalGap
		}
		if saveErr := stager.SaveTaskPage(ctx, id, checkpoint.Through, page); saveErr != nil {
			return checkpoint, saveErr
		}
		checkpoint, found = page.TaskCheckpoint, true
	}

	return checkpoint, nil
}
