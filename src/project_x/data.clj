(ns project_x.data
  (:require
    [jsonista.core :as jsonista]
    [tech.v3.dataset :as ds])
  (:import
    (java.time LocalDate )
    (java.time.format DateTimeFormatter)))


;----------------------------------------------------STATIC--------------------------------
(def api-key-fmp "SmGdEruRf6lzNhMkFeWfN4rOoMZ6AcVP")        ;personal key please do not use
(def api-key-fred "ccd6e3112e45a8be523f0aeef74da02f")       ;public key

(def endpoint-income-statement "https://financialmodelingprep.com/api/v3/income-statement/")
(def endpoint-fred-series "https://api.stlouisfed.org/fred/series/observations?")

(def start-date "2006-09-30")
(def end-date   "2025-06-30")

(def fred-series
  [; macro
   {:name "us_cpi"                  :id "CPIAUCSL"}
   {:name "umich_sent"              :id "UMCSENT"}
   {:name "payrolls"                :id "PAYEMS"}
   {:name "us_retail_sales"         :id "RSAFS"}
   ; markets / FX / rates
   {:name "usd_gbp"                 :id "DEXUSUK"}
   {:name "usd_eur"                 :id "DEXUSEU"}
   {:name "ust10y"                  :id "DGS10"}
   ;{:name "sp500"                  :id "SP500"}
   {:name "vix"                     :id "VIXCLS"}
   ; sector
   {:name "pce_recreational_goods"  :id "DREQRC1Q027SBEA"}
   {:name "pce_service"             :id "PCES"}
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
  (let [url   (str endpoint-fred-series
                   "series_id=" (:id series) "&api_key="   api-key-fred
                   "&file_type=json" "&frequency=" "q" "&limit=100000" "&aggregation_method=eop"
                   "&observation_start=" start-date "&observation_end="   end-date)
        observations (-> (slurp url) json->kmap :observations)
        mapped (map (fn [x]
                      {:orig_date (:date x)
                       :qdate     (to-eoq (:date x))
                       :series    (:name series)
                       :value     (:value x)
                       :src       "FRED"})
                    observations)]
    (map (fn [m] {:date (:qdate m) :series (:series m) :value (:value m) :src (:src m)}) mapped)))

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
        all-data  (concat fmp-data fred-data steam-data vgchartz-data)
        clean-data (->> all-data
                        (remove #(= "." (:value %)))
                        (filter #(and
                                  (not (.isBefore (parse-date (:date %)) (parse-date start-date)))
                                  (not (.isAfter (parse-date (:date %)) (parse-date end-date)))
                                  ))
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
  (ds/write! clean-data-ds "~/clean-data.csv")))