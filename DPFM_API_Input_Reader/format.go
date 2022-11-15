package dpfm_api_input_reader

import (
	"data-platform-api-currency-exconf-rmq-kube/DPFM_API_Caller/requests"
)

func (sdc *SDC) ConvertToCurrency() *requests.Currency {
	data := sdc.Currency
	return &requests.Currency{
		Currency: data.Currency,
	}
}
