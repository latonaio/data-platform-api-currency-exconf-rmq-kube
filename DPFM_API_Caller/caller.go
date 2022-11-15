package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-currency-exconf-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-currency-exconf-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-currency-exconf-rmq-kube/database"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

type ExistenceConf struct {
	ctx context.Context
	db  *database.Mysql
	l   *logger.Logger
}

func NewExistenceConf(ctx context.Context, db *database.Mysql, l *logger.Logger) *ExistenceConf {
	return &ExistenceConf{
		ctx: ctx,
		db:  db,
		l:   l,
	}
}

func (e *ExistenceConf) Conf(input *dpfm_api_input_reader.SDC) *dpfm_api_output_formatter.Currency {
	currency := *input.Currency.Currency
	notKeyExistence := make([]string, 0, 1)
	KeyExistence := make([]string, 0, 1)

	existData := &dpfm_api_output_formatter.Currency{
		Currency:      currency,
		ExistenceConf: false,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if !e.confCurrency(currency) {
			notKeyExistence = append(notKeyExistence, currency)
			return
		}
		KeyExistence = append(KeyExistence, currency)
	}()

	wg.Wait()

	if len(KeyExistence) == 0 {
		return existData
	}
	if len(notKeyExistence) > 0 {
		return existData
	}

	existData.ExistenceConf = true
	return existData
}

func (e *ExistenceConf) confCurrency(val string) bool {
	rows, err := e.db.Query(
		`SELECT Currency 
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_currency_currency_data 
		WHERE Currency = ?;`, val,
	)
	if err != nil {
		e.l.Error(err)
		return false
	}

	for rows.Next() {
		var currency string
		err := rows.Scan(&currency)
		if err != nil {
			e.l.Error(err)
			continue
		}
		if currency == val {
			return true
		}
	}
	return false
}
