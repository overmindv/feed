package rebuild

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	pb "github.com/overmindv/content/pkg/api/content"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	materialSnapshotSize = 100
	materialMessageLimit = 512 << 10
	materialCALimit      = 1 << 20
)

var ErrMaterialSnapshot = errors.New("invalid or incomplete Content snapshot page")

type MaterialSnapshot struct {
	conn  *grpc.ClientConn
	rpc   pb.ContentServiceClient
	token string
}

func NewMaterialSnapshot(address, token, caFile string, allowInsecure bool) (*MaterialSnapshot, error) {
	host, rawPort, err := net.SplitHostPort(address)
	port, portErr := strconv.ParseUint(rawPort, 10, 16)
	if err != nil || portErr != nil || port == 0 || host == "" || strings.ContainsAny(host, "/?#@\\ \t\r\n") {
		return nil, errors.New("FEED_CONTENT_GRPC_ADDRESS must contain a host and port")
	}
	if !validJournalToken(token) {
		return nil, errors.New("CONTENT_GRPC_CATALOG_TOKEN must contain 32-512 printable ASCII characters without spaces")
	}
	transport, err := materialCredentials(caFile, allowInsecure)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(transport), grpc.WithNoProxy(),
		grpc.WithDisableServiceConfig(), grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(materialMessageLimit), grpc.MaxCallSendMsgSize(materialMessageLimit)))
	if err != nil {
		return nil, errors.New("cannot open Content snapshot client")
	}

	return &MaterialSnapshot{conn: conn, rpc: pb.NewContentServiceClient(conn), token: token}, nil
}

func materialCredentials(caFile string, allowInsecure bool) (credentials.TransportCredentials, error) {
	if allowInsecure {
		if caFile != "" {
			return nil, errors.New("content CA requires TLS")
		}

		return insecure.NewCredentials(), nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := readMaterialCA(caFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM(pem) {
			return nil, errors.New("content CA contains no certificates")
		}
	}

	return credentials.NewTLS(cfg), nil
}

func readMaterialCA(path string) ([]byte, error) {
	file, err := os.Open(path) //nolint:gosec // The CA path is local operator configuration.
	if err != nil {
		return nil, errors.New("cannot open Content CA")
	}
	defer func() { _ = file.Close() }()
	pem, err := io.ReadAll(io.LimitReader(file, materialCALimit+1))
	if err != nil || len(pem) > materialCALimit {
		return nil, errors.New("cannot read bounded Content CA")
	}

	return pem, nil
}

func (s *MaterialSnapshot) Close() error { return s.conn.Close() }

func (s *MaterialSnapshot) ReadMaterialPage(ctx context.Context, watermark *int64, after string) (MaterialPage, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+s.token))
	reply, err := s.rpc.SnapshotMaterials(ctx, &pb.SnapshotMaterialsRequest{
		Watermark: watermark, AfterId: after, PageSize: materialSnapshotSize,
	})
	if err != nil {
		return MaterialPage{}, fmt.Errorf("content snapshot returned %s", status.Code(err))
	}
	if reply == nil || len(reply.GetEntries()) > materialSnapshotSize {
		return MaterialPage{}, ErrMaterialSnapshot
	}
	page := MaterialPage{Watermark: reply.GetWatermark(), NextAfterID: reply.GetNextAfterId(),
		Rows: make([]MaterialRow, 0, len(reply.GetEntries()))}
	for _, entry := range reply.GetEntries() {
		row, ok := materialSnapshotRow(entry)
		if !ok {
			return MaterialPage{}, ErrMaterialSnapshot
		}
		page.Rows = append(page.Rows, row)
	}

	return page, nil
}

func materialSnapshotRow(entry *pb.MaterialSnapshotEntry) (MaterialRow, bool) {
	if entry == nil || !validCatalogID(entry.GetMaterialId()) || !validCatalogID(entry.GetRevisionId()) {
		return MaterialRow{}, false
	}
	row := MaterialRow{
		MaterialID: entry.GetMaterialId(), RevisionID: entry.GetRevisionId(),
		Version: entry.GetVersion(), Tombstone: entry.GetTombstone(),
	}
	if stamp := entry.GetActivityOccurredAt(); stamp != nil {
		if stamp.CheckValid() != nil || stamp.AsTime().IsZero() {
			return MaterialRow{}, false
		}
		at := stamp.AsTime()
		row.OccurredAt = &at
	}
	if row.Tombstone {
		return row, true
	}
	row.Title = strings.TrimSpace(entry.GetMetadata().GetTitle())
	if row.Title == "" || strings.ContainsRune(row.Title, 0) || utf8.RuneCountInString(row.Title) > 256 {
		return MaterialRow{}, false
	}

	return row, true
}
