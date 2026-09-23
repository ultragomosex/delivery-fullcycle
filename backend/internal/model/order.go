package model

type Product struct {
	ID       int64   `json:"id"`
	Title    string  `json:"title"`
	Price    float64 `json:"price"`
	Category string  `json:"category"`
	Section  string  `json:"section"`
	Stock    int     `json:"stock"`
}

type CartItem struct {
	Product  Product `json:"product"`
	Quantity int     `json:"quantity"`
}

type Cart struct {
	UserID     int64      `json:"user_id"`
	UserEmail  string     `json:"user_email"`
	UserName   string     `json:"user_name"`
	Items      []CartItem `json:"items"`
	TotalPrice float64    `json:"total_price"`
}
