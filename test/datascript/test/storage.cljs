(ns datascript.test.storage
  (:require
   [clojure.edn :as edn]
   [clojure.test :as t :refer [is are deftest testing async]]
   [cognitect.transit :as transit]
   [me.tonsky.persistent-sorted-set-async :as set]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]
   [datascript-async.core :as d]
   [datascript-async.storage :as storage]
   [datascript.test.core :as tdc]))

(defn write-data
  [addr data]
  (case addr
    "tail" data
    "root" data
    (if (instance? set/Node data)
      #js {:keys      (.-keys ^set/Node data)
           :addresses (.-_addresses ^set/Node data)}
      #js {:keys (.-keys ^set/Leaf data)})))

(defn read-data
  [addr ^js data]
  ;; messagepack prn actually way slower for deserializing from idb
  ;; I think this prn because it copies the binary buffer into memory
  ;; then we create js objects from it
  ;; reading js objects must be really optimized from idb
  ;; now for encrypted graphs, we will have to serialize it unfortunetly :(
  (case addr
    "tail" data
    "root" data
    (let [keys (.-keys data)]
      (if-some [addresses (.-addresses data)]
        (set/Node. keys (arrays/make-array (arrays/alength addresses)) addresses)
        (set/Leaf. keys)))))


(defrecord Storage [*disk *reads *writes *deletes]
  storage/IStorage
  (-gen-addr [_ _node]
    (str (random-uuid)))
  (-accessed [_ _addr])
  (-restore [_ addr]
    (when *reads
      (vswap! *reads conj addr))
    (read-data addr (get @*disk addr)))

  (-store [_ addr+data-seq]
    (doseq [[addr data] addr+data-seq]
      (vswap! *disk assoc addr (write-data addr data))
      (when *writes
        (vswap! *writes conj addr))))
  (-list-addresses [_]
    (keys @*disk))
  (-delete [_ addrs-seq]
    (doseq [addr addrs-seq]
      (vswap! *disk dissoc addr)
      (when *deletes
        (vswap! *deletes conj addr)))))

(defn make-storage [& [opts]]
  (map->Storage
   {:*disk    (volatile! {})
    :*reads   (when (:stats opts)
                (volatile! []))
    :*writes  (when (:stats opts)
                (volatile! []))
    :*deletes (when (:stats opts)
                (volatile! []))}))

(defn reset-stats [storage]
  (vreset! (:*reads storage) [])
  (vreset! (:*writes storage) [])
  (vreset! (:*deletes storage) []))

(defn small-db [& [opts]]
  (-> (d/empty-db nil (merge {:branching-factor 1024, :ref-type :strong} opts))
      (d/db-with [[:db/add 1 :name "Ivan"]
                  [:db/add 2 :name "Oleg"]
                  [:db/add 3 :name "Petr"]])))

(defn large-db [& [opts]]
  (d/db-with
   (d/empty-db nil (merge {:branching-factor 1024, :ref-type :strong} opts))
   (map #(vector :db/add % :str (str %)) (range 1 4001))))

(deftest test-basics
  (testing "empty db"
    (let [db      (d/empty-db)
          storage (make-storage {:stats true})]
      (d/store db storage)
      (is (= 5 (count @(:*writes storage))))
      (let [db' (d/restore storage)]
        ;; we read all of the roots
        (is (= 5 (count @(:*reads storage))))   ;; read root + tail
        (is (= db db'))                         ;; read eavt
        (is (= 5 (count @(:*reads storage)))))))

  (testing "small db"
    (let [db      (small-db)
          storage (make-storage {:stats true})]
      (testing "store"
        (d/store db storage)
        (is (= 0 (count @(:*reads storage))))
        (is (= 5 (count @(:*writes storage)))))  ;; write root, tail + 1 level * 3 indexes
      (testing "restore"
        (let [db' (d/restore storage)]
          ;; this test is sort of off because we always read nodes
          (is (= 5 (count @(:*reads storage)))) ;; read root + tail
          (is (= db db'))                       ;; read eavt
          (is (= 5 (count @(:*reads storage))))
          (vec (d/datoms db' :aevt))            ;; read aevt
          (is (= 5 (count @(:*reads storage))))
          (vec (d/datoms db' :avet))            ;; read avet
          (is (= 5 (count @(:*reads storage)))))

        (testing "count"
          (reset-stats storage)
          (let [db' (d/restore storage)]
            (count db')
            (is (= 5 (count @(:*reads storage))))))

        (testing "settings"
          (let [db' (d/restore storage)]
            (is (= {:branching-factor 1024, :ref-type :strong} (d/settings db'))))))))

  (testing "large db"
    (let [db      (large-db)
          storage (make-storage {:stats true})]

      (testing "store"
        (d/store db storage)
        (is (= 19 (count @(:*writes storage))))  ;; root, tail, avet root + 8 * 2 indexes

        (d/store db)
        (is (= 19 (count @(:*writes storage))))) ;; store nothing if nothing changed

      (testing "restore"
        (let [db' (d/restore storage)]
          (is (= 5 (count @(:*reads storage)))) ;; read root + tail + root idxs

          (is (= [1 :str "1"] (-> (d/datoms db' :eavt 1) first ((juxt :e :a :v)))))
          (is (= 6 (count @(:*reads storage)))) ;; read 1 leaf

          (first (d/datoms db' :eavt))
          (is (= 12 (count @(:*reads storage)))) ;; read all leaves

          (vec (d/datoms db' :eavt))
          (is (= 12 (count @(:*reads storage)))) ;; second time no read

          (is (= db db'))
          (is (= (:eavt db) (:eavt db')))
          (is (= (:aevt db) (:aevt db')))
          (is (= (:avet db) (:avet db')))))

      (testing "count"
        (reset-stats storage)
        (let [db' (d/restore storage)]
          (is (= 4000 (count db')))
          (is (= 5 (count @(:*reads storage)))))) ;; count is stored so no extra reads

      (testing "incremental store"
        (reset-stats storage)
        (let [db' (d/db-with db [[:db/add 5001 :str "5001"]])]
          (d/store db')
          (is (= 6 (count @(:*writes storage))))))) ;; root, tail + 2 leaves * 2 indexes
    ))

#_
(deftest test-gc
  (let [storage (make-storage {:stats true})]
    (let [db (large-db {:storage storage})]
      (d/store db)
      (is (= 135 (count (d/addresses db))))
      (is (= 135 (count (storage/-list-addresses storage))))
      (is (= (d/addresses db) (set (storage/-list-addresses storage))))
      
      (let [db' (d/db-with db [[:db/add 1001 :str "1001"]])]
        (d/store db')
        (is (> (count (storage/-list-addresses storage))
               (count (d/addresses db'))))
        
        ;; no GC because both dbs are alive
        (d/collect-garbage storage)
        (is (= (into (set (d/addresses db))
                     (set (d/addresses db')))
               (set (storage/-list-addresses storage))))
        (is (= 0 (count @(:*deletes storage))))))
    
    ;; if we lose other refs, GC will happen
    (let [db'' (d/restore storage)]
      (d/collect-garbage storage)
      (is (= (d/addresses db'') (set (storage/-list-addresses storage))))
      (is (= 6 (count @(:*deletes storage)))))
    
    (testing "don’t delete currently stored db"
      (System/gc)
      (d/collect-garbage storage)
      (is (pos? (count (storage/-list-addresses storage)))))))


(deftest test-conn
  (let [storage (make-storage {:stats true})
        conn    (d/create-conn nil {:storage          storage
                                    :branching-factor 1024
                                    :ref-type         :strong})]
    (is (= 5 (count @(:*writes storage)))) ;; initial store
    
    (d/transact! conn [[:db/add 1 :name "Ivan"]])
    (is (= 6 (count @(:*writes storage))))
    (is (= @#'storage/tail-addr (last @(:*writes storage))))
    
    ;; only writing tail
    (d/transact! conn [[:db/add 2 :name "Oleg"]])
    (is (= 7 (count @(:*writes storage))))
    (is (= @#'storage/tail-addr (last @(:*writes storage))))
    (is (= 2 (count (:tx-tail @(:atom conn)))))
    (is (= 2 (count (apply concat (:tx-tail @(:atom conn))))))
    
    ;; bigger tx, still writing tail
    (d/transact! conn (mapv #(vector :db/add % :name (str %)) (range 3 1025)))
    (is (= 8 (count @(:*writes storage))))
    (is (= @#'storage/tail-addr (last @(:*writes storage))))
    (is (= 3 (count (:tx-tail @(:atom conn)))))
    (is (= 1024 (count (apply concat (:tx-tail @(:atom conn))))))
    
    ;; tail overflows, flush db
    (d/transact! conn [[:db/add 1025 :name "Petr"]])
    (is (= 16 (count @(:*writes storage))))
    
    ;; and start over
    (d/transact! conn [[:db/add 1026 :name "Anna"]])
    (is (= 17 (count @(:*writes storage))))
    (is (= @#'storage/tail-addr (last @(:*writes storage))))
    
    ;; restore conn with tail
    (let [conn' (d/restore-conn storage)]
      (is (= @conn @conn'))
      (is (= (:max-eid @conn) (:max-eid @conn')))
      (is (= (:max-tx @conn) (:max-tx @conn')))
      
      ;; transact keeps working on restored conn
      (d/transact! conn' [[:db/add 1027 :name "Vera"]])
      (is (= 18 (count @(:*writes storage))))
      (is (= @#'storage/tail-addr (last @(:*writes storage))))
      
      ;; overflow keeps working on restored conn
      (d/transact! conn' (mapv #(vector :db/add % :name (str %)) (range 1028 2051)))
      (is (= 28 (count @(:*writes storage))))
      (is (= @#'storage/tail-addr (last @(:*writes storage))))
      
      ;; restore conn without tail
      (let [conn'' (d/restore-conn storage)]
        (is (= @conn' @conn''))
        
        (d/transact! conn'' [[:db/add 2051 :name "Ilya"]])
        (is (= 29 (count @(:*writes storage))))
        (is (= @#'storage/tail-addr (last @(:*writes storage))))

        ;; gc on conn
        (is (> (count (storage/-list-addresses storage))
               (count (d/addresses (:db-last-stored @(:atom conn''))))))

        ;; TODO: GC
        #_#_
        (d/collect-garbage storage)
        (is (= (count (storage/-list-addresses storage))
               (count (d/addresses (:db-last-stored @(:atom conn''))))))
        
        (let [conn''' (d/restore-conn storage)]
          (is (= @conn'' @conn''')))))))

#_
(defn stress-test [{:keys [time branching-factor ref-type]
                    :or {time             10000
                         branching-factor 32
                         ref-type         :weak}}]
  (println "Stress-testing storage for" time "ms")
  (let [storage    (make-storage {:stats false})
        conn       (d/create-conn
                    {:idx  {:db/index true}
                     :name {:db/index true}}
                    {:storage          storage
                     :branching-factor branching-factor
                     :ref-type         ref-type})
        threads    (.availableProcessors (Runtime/getRuntime))
        exec       (Executors/newFixedThreadPool (+ 2 threads))
        *running?  (volatile! true)
        bump       (fn [db]
                     (let [op (:op (d/entity db 1))]
                       [[:db/add 1 :op (inc (or op 0))]]))
        *exception (volatile! nil)]
    ;; transact threads
    (dotimes [_ threads]
      (.submit exec ^Runnable
               (fn []
                 (try
                   (let [i (+ 2 (rand-int 100000))]
                     (d/transact! conn
                                  [[:db.fn/call bump]
                                   {:db/id i
                                    :idx   i
                                    :name  (str i)}]))
                   (catch Exception e
                     (vreset! *exception e)
                     (.printStackTrace e)))
                 (when @*running?
                   (recur)))))
    ;; JVM GC thread
    (.submit exec ^Runnable
             (fn []
               (Thread/sleep (long (rand-int 100)))
               (System/gc)
               (when @*running?
                 (recur))))
    ;; Storage GC
    (.submit exec ^Runnable
             (fn []
               (Thread/sleep 1000)
               (d/collect-garbage storage)
               (when @*running?
                 (recur))))
    (Thread/sleep (long time))
    (vreset! *running? false)
    (.shutdown exec)
    (println "  ops:" (:op (d/entity @conn 1)))
    (println "  nodes:" (count @(:*disk storage)))
    (println "  max idx:" (->> (d/datoms @conn :avet :idx) (rseq) (first) :v))
    (println "  max name:" (->> (d/datoms @conn :avet :name) (rseq) (first) :v))
    @*exception))

;; (defn -main [& {:as opts}]
;;   (let [opts' {:time             (some-> (opts "--time") Long/parseLong)
;;                :branching-factor (some-> (opts "--branching-factor") Long/parseLong)
;;                :ref-type         (some-> (opts "--ref-type") keyword)}]
;;     (when (stress-test opts')
;;       (System/exit 1))))

(comment
  (t/test-ns *ns*)
  (stress-test {:time 60000})
  (t/run-test-var #'test-conn))

(comment  
  (let [serializable (with-open [is (io/input-stream (io/file "/Users/tonsky/ws/roam/db_3M.json_transit"))]
                       (transit/read (transit/reader is :json)))]
    (def db (d/from-serializable serializable {:branching-factor 512}))
    (count db))
  
  (count db)
  
  (def storage
    (streaming-edn-storage "target/db_streaming_edn")
    #_(inmemory-edn-storage "target/db_inmemory_edn")
    #_(streaming-transit-json-storage "target/db_streaming_transit_json")
    #_(inmemory-transit-json-storage "target/db_inmemory_transit_json")
    #_(streaming-transit-msgpack-storage "target/db_streaming_transit_msgpack"))
  
  (d/store db storage)
  
  (def db'
    (d/restore storage))
  
  (count (d/addresses db))
  (count (d/addresses db'))
  (count (storage/-list-addresses storage))
  (d/collect-garbage storage)

  (first (:eavt db'))
  
  (->> (:eavt db')
       (drop 5000)
       (take 5000)))  
