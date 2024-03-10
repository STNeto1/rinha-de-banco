package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB models
type DBClient struct {
	ID      int
	Limit   int64
	Balance int64
}

type DBTransaction struct {
	ID          int
	ClientID    int
	Amount      int64
	Type        string
	Description string
	CreatedAt   pgtype.Timestamp
}

// Create transaction models
type TransactionPayload struct {
	Value       int32  `json:"valor"`
	Type        string `json:"tipo"`
	Description string `json:"descricao"`
}

type CreateTransactionOutput struct {
	Balance int64 `json:"saldo"`
	Limit   int64 `json:"limite"`
}

// Get extract models
type ExtractOutputModel struct {
	Saldo struct {
		Total int64  `json:"total"`
		Date  string `json:"data_extrato"`
		Limit int64  `json:"limite"`
	} `json:"saldo"`

	LastTransactions []TransactionOutput `json:"ultimas_transacoes"`
}

type TransactionOutput struct {
	Amount      int64            `json:"valor"`
	Type        string           `json:"tipo"`
	Description string           `json:"descricao"`
	CreatedAt   pgtype.Timestamp `json:"data"`
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
			return c.Status(http.StatusNotFound).SendString("Cliente não encontrado")
		}

		var body TransactionPayload
		if err := c.BodyParser(&body); err != nil {
			return c.Status(http.StatusUnprocessableEntity).SendString("Erro ao processar o corpo requisição")
		}

		if body.Value < 1 {
			return c.Status(http.StatusUnprocessableEntity).SendString("Valor inválido")
		}

		if body.Description == "" || len(body.Description) > 10 {
			return c.Status(http.StatusUnprocessableEntity).SendString("Descrição inválida")
		}

		if body.Type != "c" && body.Type != "d" {
			return c.Status(http.StatusUnprocessableEntity).SendString("Tipo inválido")
		}

		tx, err := dbpool.BeginTx(c.Context(), pgx.TxOptions{})
		if err != nil {
			log.Println("error creating trx", err)
			return c.Status(http.StatusInternalServerError).SendString("Erro gerando transação")
		}
		defer tx.Rollback(c.Context())

		if _, err := tx.Exec(c.Context(), "INSERT INTO transactions (client_id, amount, type, description) VALUES ($1, $2, $3, $4)", id, body.Value, body.Type, body.Description); err != nil {
			if strings.Contains(err.Error(), "clients_balance_check") {
				return c.Status(422).SendString("Saldo inválido pós transação")
			}

			log.Println("Erro processando transação", err, body)
			return c.Status(400).SendString("Erro ao processar a resposta da requisição ao banco")
		}

		if body.Type == "c" {
			if _, err := tx.Exec(c.Context(), "UPDATE clients SET balance = balance + $1 WHERE id = $2", body.Value, id); err != nil {
				if strings.Contains(err.Error(), "clients_balance_check") {
					return c.Status(422).SendString("Saldo inválido pós transação")
				}

				log.Println("Erro processando transação", err, body)
				return c.Status(400).SendString("Erro ao processar a resposta da requisição ao banco")
			}
		} else {

			if _, err := tx.Exec(c.Context(), "UPDATE clients SET balance = balance - $1 WHERE id = $2", body.Value, id); err != nil {
				if strings.Contains(err.Error(), "clients_balance_check") {
					return c.Status(422).SendString("Saldo inválido pós transação")
				}

				log.Println("Erro processando transação", err, body)
				return c.Status(400).SendString("Erro ao processar a resposta da requisição ao banco")
			}
		}

		var clientData DBClient
		if err = tx.QueryRow(c.Context(), "select * from clients where id = $1 limit 1", id).Scan(&clientData.ID, &clientData.Limit, &clientData.Balance); err != nil {
			log.Println("error fetching client data", err)
			return c.Status(http.StatusInternalServerError).SendString("Erro ao buscar dados do cliente")
		}

		if err := tx.Commit(c.Context()); err != nil {
			log.Println("error commiting", err)
			return c.Status(http.StatusInternalServerError).SendString("Erro ao buscar dados do cliente")
		}

		return c.Status(200).JSON(CreateTransactionOutput{
			Balance: clientData.Balance,
			Limit:   clientData.Limit,
		})
	})

	app.Get("/clientes/:id/extrato", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")

		// Funciona né K
		if err != nil || id <= 0 || id > 5 {
			log.Println("Id param", id, err)
			return c.Status(http.StatusNotFound).SendString("Cliente não encontrado")
		}

		tx, err := dbpool.BeginTx(c.Context(), pgx.TxOptions{})
		if err != nil {
			log.Println("error creating trx", err)
			return c.Status(http.StatusInternalServerError).SendString("Erro gerando transação")
		}
		defer tx.Rollback(c.Context())

		var clientData DBClient
		if err = tx.QueryRow(c.Context(), "select * from clients where id = $1", id).Scan(&clientData.ID, &clientData.Limit, &clientData.Balance); err != nil {
			log.Println("error fetching client data", err)
			return c.Status(http.StatusInternalServerError).SendString("Erro ao buscar dados do cliente")
		}

		rows, err := tx.Query(c.Context(), "select * from transactions where client_id = $1 order by created_at desc", id)
		if err != nil {
			log.Println("error fetching  transactions", err)
			return c.Status(http.StatusInternalServerError).SendString("Erro ao buscar dados da transação")
		}

		transactions, err := pgx.CollectRows(rows, pgx.RowToStructByName[DBTransaction])
		if err != nil {
			log.Println("error collecting transactions", err)
			return c.Status(http.StatusInternalServerError).SendString("Erro ao buscar dados da transação")
		}

		var result ExtractOutputModel
		result.Saldo.Total = clientData.Balance
		result.Saldo.Limit = clientData.Limit
		result.Saldo.Date = time.Now().Format("2006-01-02")

		for _, transaction := range transactions {
			result.LastTransactions = append(result.LastTransactions, TransactionOutput{
				Amount:      transaction.Amount,
				Type:        transaction.Type,
				Description: transaction.Description,
				CreatedAt:   transaction.CreatedAt,
			})
		}

		if result.LastTransactions == nil {
			result.LastTransactions = []TransactionOutput{}
		}

		return c.Status(200).JSON(result)
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
