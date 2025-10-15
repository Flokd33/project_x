(ns project_x.data
  (:require
    ;[project_x.static :as static]
    [project_x.tools :as t]
    )
  (:import java.time.LocalDate)
  )


;----------------------------------------------------FMP--------------------------------------------------------
(def api-key-fmp "apikey=SmGdEruRf6lzNhMkFeWfN4rOoMZ6AcVP")

(def endpoint-balance-sheet-statement "https://financialmodelingprep.com/api/v3/balance-sheet-statement/")
(def endpoint-income-statement "https://financialmodelingprep.com/api/v3/income-statement/")
(def endpoint-cash-flow-statement "https://financialmodelingprep.com/api/v3/cash-flow-statement/")

(defn get-balance-sheet-statement [ticker]   (t/json->kmap (slurp (str endpoint-balance-sheet-statement  ticker "?period=quarter&" api-key-fmp))))
(defn get-income-statement [ticker]          (t/json->kmap (slurp (str endpoint-income-statement         ticker "?period=quarter&" api-key-fmp))))
(defn get-cash-flow-statement [ticker]       (t/json->kmap (slurp (str endpoint-cash-flow-statement      ticker "?period=quarter&" api-key-fmp))))

(def out1 (get-income-statement "EA"))

;----------------------------------------------------FRED--------------------------------------------------------
(def api-key-fred "ccd6e3112e45a8be523f0aeef74da02f")

(def endpoint-fred-series "https://api.stlouisfed.org/fred/series/observations?")

(defn get-fred-series [id] (t/json->kmap (slurp (str endpoint-fred-series "series_id=" id "&api_key=" api-key-fred "&file_type=json"))))

(def out2 (:observations (get-fred-series "DGS10")))

;FRED’s “game” tag shows