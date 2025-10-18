(ns project_x.data
  (:require
    [clj-http.client :as http]
    [jsonista.core :as jsonista]
    [tech.v3.dataset :as ds]
    [clojure.string :as str]
    [clojure.java.io :as io]
    )
  (:import
    (java.time LocalDate Year Month Instant ZoneId ZonedDateTime)
    (java.time.format DateTimeFormatter)))


(comment
  "ETL process using FMP + FRED, extract .csv for R modelling
   Starting from 09/2006 as this is the first steam data available => ~19 years of data

  Dependent variable
  • ea_rev_q (FMP, quarterly income statement) — Proxy for *EA’s financial performance*, chosen because revenue is the most direct and least noisy link to external demand and FX translation effects.

  Macroeconomic & market indicators (FRED)
  • us_cpi (CPIAUCSL) — Proxy for *consumer purchasing power / inflation*; captures real cost-of-living pressures that affect discretionary spending on games.
  • umich_sent (UMCSENT) — Proxy for *consumer confidence / sentiment*; gauges forward-looking household optimism that drives discretionary purchases.
  • payrolls (PAYEMS) — Proxy for *labor-market strength and income base*; rising employment supports household income and gaming spend.
  • us_retail_sales (RSAFS) — Proxy for *aggregate consumer demand / discretionary consumption*; correlated with video game and hardware sales.
  • usd_gbp (DEXUSUK) — Proxy for *foreign-exchange translation risk*; EA reports in USD but sells significantly in the UK and Europe, so GBP/USD captures translation effects on reported revenue.
  • usd-eur
  • ust10y (DGS10) — Proxy for *macro discount rate / policy stance*; higher yields reflect tighter financial conditions and can weigh on consumer and market sentiment.
  • vix (VIXCLS) — Proxy for *market risk aversion / volatility sentiment*; spikes in VIX align with global risk-off periods that often dampen discretionary consumption.

  Sector / industry indicator (FRED, quarterly)
  • pce_recreactional_goods (DREQRC1Q027SBEA) - U.S. Personal Consumption Expenditures on Durable Recreational Goods & Vehicles as a long-history quarterly stand-in for gaming hardware demand.
  • pce_service (PCES) - Personal Consumption Expenditures: Services
  • Steam users from Steamdb
  • Hardware sales index -> total of all console sales from VGChartz

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

(def start-date "2006-09-30")
(def end-date   "2025-06-30")

(def fred-series
  [; macro
   {:name "us_cpi"          :id "CPIAUCSL"}                 ; CPI => proxy for purchasing power
   {:name "umich_sent"      :id "UMCSENT"}                  ; proxy for consumer sentiment
   {:name "payrolls"        :id "PAYEMS"}                   ; nonfarm payrolls => proxy for labour demand
   {:name "us_retail_sales" :id "RSAFS"}                    ; US retail sales => proxy for consumer demand
   ; markets / FX / rates
   {:name "usd_gbp"         :id "DEXUSUK"}                  ; EA reports in USD, but EU/UK sales are meaningful. Broad trade-weighted U.S. Dollar Index (DTWEXBGS) and USD/EUR (DEXUSEU) do not have 30 years of history
   {:name "usd_eur"         :id "DEXUSEU"}
   {:name "ust10y"          :id "DGS10"}                    ; 10y treasury => discount rate proxy
   ;{:name "sp500"           :id "SP500"}                   ; risk appetite proxy
   {:name "vix"             :id "VIXCLS"}                   ; risk/vol sentiment proxy
   ; sector
   {:name "pce_recreational_goods" :id "DREQRC1Q027SBEA"}      ; pce_recreational_goods: Durable goods: Recreational goods and vehicles
   {:name "pce_service" :id "PCES"}                         ;Personal Consumption Expenditures: Services
   ;{:name "ppi_game_software"    :id "PCU5112105112107" :frequency "m"} ;Producer Price Index by Industry: Software Publishers: Game Software Publishing
   ])

;----------------------------------------------------TOOLS--------------------------------------------------------------
(defn json->kmap [^String s] (jsonista/read-value s (jsonista/object-mapper {:decode-key-fn keyword})))

(defn clean-key [k]
  (-> k name
      (clojure.string/replace #"\"" "")   ; remove double quotes
      keyword))

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
            :value (str (:revenue m))     ; FMP fields we can use -> :revenue :grossProfit :ebitda :netIncome :researchAndDevelopmentExpenses :researchanddevelopmentexpensesoftwareexcludingacquiredinprocesscost
            :src "FMP"
            })
         res)))
;----------------------------------------------------FRED---------------------------------------------------------------
(defn get-fred-series [series]
  "Get data for a given FRED series map {:id ... :name ... :frequency ...}
   - if :frequency is \"m\" we will fetch monthly and collapse to quarter-end by taking the last month in each quarter."
  (let [freq  (or (:frequency series) "q")
        url   (str endpoint-fred-series
                   "series_id=" (:id series)
                   "&api_key="   api-key-fred
                   "&file_type=json"
                   "&frequency=" freq
                   "&limit=100000"
                   "&aggregation_method=eop"
                   "&observation_start=" start-date
                   "&observation_end="   end-date)
        observations (-> (slurp url) json->kmap :observations)
        mapped (map (fn [x]
                      {:orig_date (:date x)         ; original monthly/quarter date returned by FRED
                       :qdate     (to-eoq (:date x)) ; mapped to quarter-end (yyyy-MM-dd)
                       :series    (:name series)
                       :value     (:value x)
                       :src       "FRED"})
                    observations)]
    (if (= freq "m")
      (->> mapped
           (group-by :qdate)
           (map (fn [[qdate vals]]
                  (let [latest (apply max-key #(parse-date (:orig_date %)) vals)]
                    {:date  qdate
                     :series (:series latest)
                     :value (:value latest)
                     :src  (:src latest)})))
           (sort-by #(parse-date (:date %))))
      (map (fn [m] {:date (:qdate m) :series (:series m) :value (:value m) :src (:src m)}) mapped))))

;----------------------------------------------------VGChartz-------------------------------------------------
;Monthly global hardware data, by platform
;https://www.vgchartz.com/tools/hw_date.php?reg=Global&ending=Monthly

(defn get-vgchartz-data []
  (let [raw-html (slurp "resources/vgchartz_raw_hardware.txt")
        series-matches (re-seq #"(?s)name:'([^']+)'\s*,\s*data\s*:\s*\[(.*?)\]" raw-html)
        epoch->date (fn [ms]
                      (-> (java.time.Instant/ofEpochMilli (Long/parseLong ms))
                          (java.time.LocalDateTime/ofInstant (java.time.ZoneOffset/UTC))
                          .toLocalDate
                          str))

        q? (fn [date-str]
             (let [m (.getMonthValue (java.time.LocalDate/parse date-str))]
               (contains? #{3 6 9 12} m)))

        per-console (mapcat
                      (fn [[_ console block]]
                        (map (fn [[_ x y]]
                               {:date   (epoch->date x)
                                :series (str "vg_" (clojure.string/replace console " " "_"))
                                :value  (str y)
                                :src    "VGCHARTZ"})
                             (re-seq #"(?s)\{\s*x\s*:\s*(\d+)\s*,\s*y\s*:\s*(\d+)\s*\}" block)))
                      series-matches)

        ;; keep only quarter-end months and normalize date to month-end
        eoq-points (->> per-console
                        (filter #(q? (:date %)))
                        (map #(update % :date to-eoq)))

        totals (map (fn [[d xs]]
                      {:date   d
                       :series "vg_hardware_total"
                       :value  (str (reduce + (map #(Long/parseLong (:value %)) xs)))
                       :src    "VGCHARTZ"})
                    (group-by :date eoq-points))]
    (->> totals                         ;we just keep the total as consoles are changing
         (sort-by :date))))
;----------------------------------------------------STEAMDB-------------------------------------------------
; Steam users since 2004
;https://steamdb.info/app/753/charts/#18y

(defn get-steam-data []
  (let [raw-data (ds/->dataset "resources/steamdb_raw_users.csv" {:key-fn clean-key})
        clean    (->> (ds/rows raw-data)
                      (remove #(nil? (:Users %))))
        mapped   (map (fn [row]
                        {:dateTime (:DateTime row)
                         :date     (to-eoq (subs (str (:DateTime row)) 0 10)) ; EOQ
                         :series   "steam_users"
                         :value    (str (:Users row))
                         :src      "STEAMDB"})
                      clean)]
    ; sort by dateTime and keep latest record
    (->> mapped
         (sort-by :dateTime)
         (group-by :date)
         (map (fn [[_ vals]] (last vals)))
         (map #(dissoc % :dateTime))
         (sort-by :date))))

;----------------------------------------------------FETCH AND CLEAN-------------------------------------------------
(defn fetch-all-data []
  (let [fmp-data  (get-income-statement "EA")
        fred-data (mapcat get-fred-series fred-series)
        steam-data (get-steam-data)
        vgchartz-data (get-vgchartz-data)
        all-data  (concat fmp-data fred-data steam-data vgchartz-data)              ;steam-data vgchartz-data
        clean-data (->> all-data
                        (remove #(= "." (:value %))) ;these are current quarter FRED values
                        (filter #(and
                                  (not (.isBefore (parse-date (:date %)) (parse-date start-date)))
                                  (not (.isAfter (parse-date (:date %)) (parse-date end-date)))
                                  )) ; keep only dates >= start-date, FMP doesn't have start date on income statement
                        )
        df-data (ds/->dataset clean-data {:key-fn keyword})
        x (println (let [vals (map (fn [date] (count (second date)) ) (group-by :date clean-data))]
                    (str "avg count per date: " (/ (reduce + vals) (count vals) 1.0))
                    ))
        ]
    df-data))

;----------------------------------------------------SAVE--------------------------------------------------------
(defn save-clean-data! []
 (let [clean-data-ds (fetch-all-data) ]
  (ds/write! clean-data-ds "resources/clean-data.csv")))
