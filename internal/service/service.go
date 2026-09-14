package service

import (
	"context"

	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/repository"
)

// Service — бизнес-логика ленты активностей.
type Service struct {
	repo repository.Repository
}

// New создаёт сервис ленты.
func New(repo repository.Repository) *Service {
	return &Service{repo: repo}
}

// ListFeed возвращает записи ленты по убыванию времени события.
func (s *Service) ListFeed(ctx context.Context, limit, offset int) ([]domain.FeedItem, error) {
	return s.repo.ListItems(ctx, limit, offset)
}
