package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TransactionPayload struct {
	Value       int32  `json:"valor"`
	Type        string `json:"tipo"`
	Description string `json:"descricao"`
}

func main() {
	port := os.Getenv("PORT")
	dsn := os.Getenv("DATABASE_URL")

	if port == "" {
		port = "9999"
	}
	if dsn == "" {
		dsn = "postgresql://postgres:postgres@localhost:5432/rinha?sslmode=disable"
	}

	dbpool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatal("failed to open => ", err)
	}
	defer dbpool.Close()

	app := fiber.New(fiber.Config{
		JSONEncoder: sonic.Marshal,
		JSONDecoder: sonic.Unmarshal,
	})

	app.Post("/clientes/:id/transacoes", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")

		// Funciona né K
		if err != nil || id <= 0 || id > 5 {
			return c.Status(404).SendString("Cliente não encontrado")
		}

		var body TransactionPayload
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).SendString("Erro ao processar o corpo requisição")
		}

		if body.Description == "" || len(body.Description) > 10 {
			return c.Status(422).SendString("Descrição inválida")
		}

		if body.Type != "c" && body.Type != "d" {
			return c.Status(422).SendString("Tipo inválido")
		}

		var response json.RawMessage
		err = dbpool.QueryRow(context.Background(), "select * from create_transaction($1, $2, $3, $4)", id, body.Value, body.Type, body.Description).Scan(&response)

		if err != nil {
			if strings.Contains(err.Error(), "clients_balance_check") {
				return c.Status(422).SendString("Valor inválido")
			}

			log.Println("Erro processando transação", err, body)

			return c.Status(400).SendString("Erro ao processar a resposta da requisição ao banco")
		}

		return c.Status(200).Send(response)
	})

	app.Get("/clientes/:id/extrato", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")

		// Funciona né K
		if err != nil || id <= 0 || id > 5 {
			log.Println("Id param", id, err)
			return c.Status(404).SendString("Cliente não encontrado")
		}

		var response json.RawMessage
		if err := dbpool.QueryRow(context.Background(), "select * from get_extract($1)", id).Scan(&response); err != nil {
			log.Println("Scan err", err)
			return c.Status(400).SendString("Erro ao processar a resposta da requisição ao banco")
		}

		return c.Status(200).Send(response)
	})

	app.Get("/reset", func(c *fiber.Ctx) error {
		if _, err := dbpool.Exec(context.Background(), "select reset_data()"); err != nil {
			log.Println("Scan err", err)
			return c.Status(400).SendString("Erro ao processar a resposta da requisição ao banco")
		}

		return c.Status(200).SendString("Resetado")
	})

	app.Listen(fmt.Sprintf(":%s", port))
}
