package catalog

import "delivery-fullcycle/internal/model"

var Products = []model.Product{
	{ID: 1, Name: "Пицца Маргарита", Price: 590.00},
	{ID: 2, Name: "Пицца Пепперони", Price: 690.00},
	{ID: 3, Name: "Ролл Калифорния", Price: 450.00},
	{ID: 4, Name: "Ролл Филадельфия", Price: 520.00},
	{ID: 5, Name: "Бургер Классик", Price: 380.00},
	{ID: 6, Name: "Бургер Чизбургер", Price: 420.00},
	{ID: 7, Name: "Цезарь с курицей", Price: 350.00},
	{ID: 8, Name: "Том Ям", Price: 480.00},
	{ID: 9, Name: "Кола 0.5л", Price: 120.00},
	{ID: 10, Name: "Сок апельсиновый", Price: 150.00},
}

func GetAll() []model.Product {
	return Products
}
