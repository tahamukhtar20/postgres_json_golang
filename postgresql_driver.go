package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

type Row map[string]interface{}

type QueryResponse struct {
	StatusCode int         `json:"status_code"`
	Message    string      `json:"message,omitempty"`
	Data       interface{} `json:"data,omitempty"`
	Error      string      `json:"error_message,omitempty"`
}

type Driver struct {
	db *sql.DB
}

func PJConverter(host string, port int, database string, user string, password string) (*Driver, error) {
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		host, port, database, user, password)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	driver := &Driver{db: db}
	return driver, nil
}

func (d *Driver) Close() {
	if d.db != nil {
		err := d.db.Close()
		if err != nil {
			log.Println("Error closing the database connection:", err)
		}
	}
}

func (d *Driver) Query(query string) (string, error) {
	command := strings.ToUpper(strings.TrimSpace(strings.Split(query, " ")[0]))

	switch command {
	case "UPDATE", "CREATE", "INSERT", "DELETE", "DROP":
		_, err := d.db.Exec(query)
		if err != nil {
			return "", err
		}
		return formatQueryResponse(200, "Query executed successfully.", nil), nil
	case "SELECT":
		rows, err := d.db.Query(query)
		if err != nil {
			return "", err
		}
		defer func(rows *sql.Rows) {
			err := rows.Close()
			if err != nil {
				log.Println("Error closing the rows:", err)
			}
		}(rows)

		results, err := processQueryResults(rows)
		if err != nil {
			return "", err
		}

		if len(results) > 0 {
			return formatQueryResponse(200, "", results), nil
		} else {
			return formatQueryResponse(204, "No data found.", nil), nil
		}
	default:
		return formatQueryResponse(400, "Unsupported SQL command.", nil), nil
	}
}

func processQueryResults(rows *sql.Rows) ([]Row, error) {
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Println("Error closing the rows:", err)
		}
	}(rows)

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := make([]Row, 0)

	for rows.Next() {
		row, err := scanRow(rows, columns)
		if err != nil {
			return nil, err
		}

		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func scanRow(rows *sql.Rows, columns []string) (Row, error) {
	values := make([]interface{}, len(columns))
	valuePointers := make([]interface{}, len(columns))

	for i := range columns {
		valuePointers[i] = &values[i]
	}

	err := rows.Scan(valuePointers...)
	if err != nil {
		return nil, err
	}

	row := make(Row)

	for i, col := range columns {
		value := values[i]

		switch v := value.(type) {
		case []byte:
			if intValue, err := convertToInt(string(v)); err == nil {
				row[col] = intValue
			} else if floatValue, err := strconv.ParseFloat(string(v), 64); err == nil {
				row[col] = floatValue
			} else {
				row[col] = string(v)
			}
		case int64:
			row[col] = v
		case float64:
			row[col] = v
		case string:
			row[col] = v
		case nil:
			row[col] = nil
		default:
			return nil, fmt.Errorf("unsupported column type: %T", v)
		}
	}

	return row, nil
}

func convertToInt(value string) (int, error) {
	numericValue, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	return numericValue, nil
}

func formatQueryResponse(statusCode int, message string, data interface{}) string {
	response := QueryResponse{
		StatusCode: statusCode,
		Message:    message,
		Data:       data,
	}

	jsonResult, err := json.Marshal(response)
	if err != nil {
		log.Println("Error marshaling query response:", err)
		return ""
	}

	return string(jsonResult)
}

func main() {
	driver, err := PJConverter("localhost", 5432, "test", "postgres", "postgres")
	if err != nil {
		log.Fatal("Error establishing the database connection:", err)
	}
	defer driver.Close()

	query := "SELECT * FROM public.user_table"
	result, err := driver.Query(query)
	if err != nil {
		log.Println("Error executing the query:", err)
	}

	fmt.Println(result)
}
