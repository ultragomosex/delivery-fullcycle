package model

type Product struct {
	ID    int
	Name  string
	Price float64
}

type CartItem struct {
	Product  Product
	Quantity int
}

type Cart struct {
	ClientID string
	Items    []CartItem
}
