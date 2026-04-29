package usecase

import (
	"errors"
	"fmt"
	"log"

	"github.com/google/uuid"
	"payment-service/internal/domain"
)

// PaymentUseCase contains all payment business logic
type PaymentUseCase struct {
	repo      PaymentRepository
	publisher EventPublisher // nil-safe: publishing is best-effort
}

func NewPaymentUseCase(repo PaymentRepository, publisher EventPublisher) *PaymentUseCase {
	return &PaymentUseCase{repo: repo, publisher: publisher}
}

// Authorize processes a payment request and publishes a PaymentEvent on success.
func (uc *PaymentUseCase) Authorize(orderID string, amount int64) (*domain.Payment, error) {
	status := domain.StatusAuthorized
	if amount > domain.MaxAmount {
		status = domain.StatusDeclined
	}

	payment := &domain.Payment{
		ID:            uuid.NewString(),
		OrderID:       orderID,
		TransactionID: uuid.NewString(),
		Amount:        amount,
		Status:        status,
	}

	if err := uc.repo.Save(payment); err != nil {
		return nil, err
	}

	// Publish event after DB commit — only for authorized payments
	if status == domain.StatusAuthorized && uc.publisher != nil {
		event := domain.PaymentEvent{
			EventID:       uuid.NewString(),
			OrderID:       payment.OrderID,
			Amount:        payment.Amount,
			CustomerEmail: fmt.Sprintf("customer-%s@example.com", payment.OrderID[:8]),
			Status:        payment.Status,
		}
		if err := uc.publisher.PublishPaymentEvent(event); err != nil {
			// Log but don't fail: payment is already committed
			log.Printf("WARNING: failed to publish payment event: %v", err)
		}
	}

	return payment, nil
}

// GetByOrderID returns the payment for a given order
func (uc *PaymentUseCase) GetByOrderID(orderID string) (*domain.Payment, error) {
	return uc.repo.FindByOrderID(orderID)
}

// ListByAmountRange returns payments filtered by amount range.
func (uc *PaymentUseCase) ListByAmountRange(min, max int64) ([]*domain.Payment, error) {
	if min > 0 && max > 0 && min > max {
		return nil, errors.New("min_amount cannot be greater than max_amount")
	}
	return uc.repo.FindByAmountRange(min, max)
}
