package model

import "testing"

func TestCartItemInput_ResolveProductID(t *testing.T) {
	t.Run("direct product id", func(t *testing.T) {
		item := CartItemInput{ProductID: 42}
		if got := item.ResolveProductID(); got != 42 {
			t.Fatalf("expected 42, got %d", got)
		}
	})

	t.Run("nested product id", func(t *testing.T) {
		item := CartItemInput{}
		item.Product.ID = 99
		if got := item.ResolveProductID(); got != 99 {
			t.Fatalf("expected 99, got %d", got)
		}
	})
}
