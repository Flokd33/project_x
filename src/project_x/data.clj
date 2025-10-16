(ns project_x.data
  (:require
    [clj-http.client :as http]
    [jsonista.core :as jsonista]
    [tech.v3.dataset :as ds]
    )
  (:import
    (java.time LocalDate Year Month)
    (java.time.format DateTimeFormatter)))


(comment
  "ETL process using FMP + FRED, extract .csv for R modelling
  30 years of quarterly data

  Dependent variable
  • ea_rev_q (FMP, quarterly income statement) — Proxy for *EA’s financial performance*, chosen because revenue is the most direct and least noisy link to external demand and FX translation effects.

  Macroeconomic & market indicators (FRED)
  • us_cpi (CPIAUCSL) — Proxy for *consumer purchasing power / inflation*; captures real cost-of-living pressures that affect discretionary spending on games.
  • umich_sent (UMCSENT) — Proxy for *consumer confidence / sentiment*; gauges forward-looking household optimism that drives discretionary purchases.
  • payrolls (PAYEMS) — Proxy for *labor-market strength and income base*; rising employment supports household income and gaming spend.
  • us_retail_sales (RSAFS) — Proxy for *aggregate consumer demand / discretionary consumption*; correlated with video game and hardware sales.
  • usd_gbp (DEXUSUK) — Proxy for *foreign-exchange translation risk*; EA reports in USD but sells significantly in the UK and Europe, so GBP/USD captures translation effects on reported revenue.
  • ust10y (DGS10) — Proxy for *macro discount rate / policy stance*; higher yields reflect tighter financial conditions and can weigh on consumer and market sentiment.
  • vix (VIXCLS) — Proxy for *market risk aversion / volatility sentiment*; spikes in VIX align with global risk-off periods that often dampen discretionary consumption.

  Sector / industry indicator (FRED, quarterly)
  • gaming_equipment_pce (DREQRC1Q027SBEA) — Proxy for *industry-specific hardware and console cycle*; uses U.S. Personal Consumption Expenditures on Durable Recreational Goods & Vehicles as a long-history quarterly stand-in for gaming hardware demand.
  • The best mainstream sector proxy is Personal consumption expenditures (DIPERC1A027NBEA): Video and audio equipment, computers, and related services: Information processing equipment BUT only annual data available
  • We could also create a console_cycle_index using VGChartz Global Hardware Sales (Weekly estimates for PlayStation, Xbox, Nintendo) BUT data only available from 2020...

  Implementation notes
  • Frequency & dating: FRED requested as quarterly end-of-period; normalized to calendar quarter-end using (to-eoq).
  • Cleaning: drop FRED’s “.” placeholders (incomplete current quarter), filter to start-date, and ensure each date includes all variables.
  • Units: left in native form; growth (YoY) and change (Δ) transformations applied in R.
  • Coverage decisions: USD/GBP retained for long FX history; SP500 omitted to avoid redundancy with sentiment/rate proxies.

  Assumptions & limitations
  • U.S. macro used as proxy for global demand (EA’s revenue split ≈ 40% North America / 60% International).
  • Sector proxy broadens to recreational durables to ensure full quarterly coverage before 2010.

  Output
  • One tidy quarterly dataset (CSV) — ready for R ingestion."
  )

;----------------------------------------------------STATIC--------------------------------
(def api-key-fmp "SmGdEruRf6lzNhMkFeWfN4rOoMZ6AcVP")
(def api-key-fred "ccd6e3112e45a8be523f0aeef74da02f")

(def endpoint-income-statement "https://financialmodelingprep.com/api/v3/income-statement/")
(def endpoint-fred-series "https://api.stlouisfed.org/fred/series/observations?")

(def start-date "1995-09-30")
(def end-date   "2025-06-30")

(def fred-series
  [; macro
   {:name "us_cpi"          :id "CPIAUCSL"}                 ; CPI => proxy for purchasing power
   {:name "umich_sent"      :id "UMCSENT"}                  ; proxy for consumer sentiment
   {:name "payrolls"        :id "PAYEMS"}                   ; nonfarm payrolls => proxy for labour demand
   {:name "us_retail_sales" :id "RSAFS"}                    ; US retail sales => proxy for consumer demand
   ; markets / FX / rates
   {:name "usd_gbp"         :id "DEXUSUK"}                  ; EA reports in USD, but EU/UK sales are meaningful. Broad trade-weighted U.S. Dollar Index (DTWEXBGS) and USD/EUR (DEXUSEU) do not have 30 years of history
   {:name "ust10y"          :id "DGS10"}                    ; 10y treasury => discount rate proxy
   ;{:name "sp500"           :id "SP500"}                   ; risk appetite proxy
   {:name "vix"             :id "VIXCLS"}                   ; risk/vol sentiment proxy
   ; sector
   {:name "gaming_equipment_pce":id "DREQRC1Q027SBEA"}      ; Personal consumption expenditures: Durable goods: Recreational goods and vehicles
   ])

;----------------------------------------------------TOOLS--------------------------------------------------------------
(defn json->kmap [^String s] (jsonista/read-value s (jsonista/object-mapper {:decode-key-fn true})))

(def iso (DateTimeFormatter/ofPattern "yyyy-MM-dd"))

(defn parse-date [^String s] (LocalDate/parse s iso))
(defn fmt-date [^LocalDate d] (.format d iso))

(defn last-day-of-quarter [^LocalDate d]
  (let [m (.getMonthValue d)
        q (cond (<= m 3) 3 (<= m 6) 6 (<= m 9) 9 :else 12)
        y (.getYear d)
        last-day (.lengthOfMonth (LocalDate/of y q 1))]
    (LocalDate/of y q last-day)))

(defn to-eoq [ s] (-> s parse-date last-day-of-quarter fmt-date))

;----------------------------------------------------FMP----------------------------------------------------------------
(defn get-income-statement [ticker]
  "Get quarterly income statement data for a given ticker"
  (let [url (str endpoint-income-statement
                 ticker
                 "?period=quarter"
                 "&apikey=" api-key-fmp)
        res (json->kmap (slurp url))]
    (map (fn [m]
           {:date (to-eoq (:date m))     ; fiscal quarter end dates that don’t perfectly align to calendar EOQ. (:fillingDate m)
            :series "ea_rev_q"
            :value (str (:revenue m))     ; FMP fields we can use -> :revenue :grossProfit :ebitda :netIncome
            :src "FMP"
            })
         res)))
;----------------------------------------------------FRED---------------------------------------------------------------
(defn get-fred-series [series]
  "Get quarterly data (EOP) for a given FRED series map {:id ... :name ...}"
  (let [url  (str endpoint-fred-series
                  "series_id=" (:id series)
                  "&api_key="   api-key-fred
                  "&file_type=json"
                  "&frequency=q"
                  "&limit=100000"
                  "&aggregation_method=eop"
                  "&observation_start=" start-date
                  "&observation_end="   end-date)
        res  (-> (slurp url) json->kmap :observations)]
    (map (fn [x]
           {:date   (to-eoq (:date x))   ;the value is end-of-quarter, but the date FRED returns is the first day of the quarter
            :series (:name series)
            :value  (:value x)
            :src    "FRED"})
         res)))

;----------------------------------------------------FETCH AND CLEAN-------------------------------------------------
(defn fetch-all-data []
  (let [fmp-data  (get-income-statement "EA")
        fred-data (mapcat get-fred-series fred-series)
        all-data  (concat fmp-data fred-data)
        clean-data (->> all-data
                        (remove #(= "." (:value %))) ;these are current quarter FRED values
                        (filter #(not (.isBefore (parse-date (:date %)) (parse-date start-date)))) ; keep only dates >= start-date, FMP doesn't have start date on income statement
                        )
        df-data (ds/->dataset clean-data {:key-fn keyword})
        x (println (let [vals (map (fn [date] (count (second date)) ) (group-by :date clean-data))]
                    (str "avg count per date: " (/ (reduce + vals) (count vals)))
                    ))
        ]
    df-data))

;----------------------------------------------------SAVE--------------------------------------------------------
(defn save-clean-data! []
 (let [clean-data-ds (fetch-all-data) ]
  (ds/write! clean-data-ds "resources/clean-data.csv")))



