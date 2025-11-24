(ns datascript.test.storage
  (:require
   [promesa.core :as p]
   [clojure.edn :as edn]
   [clojure.test :as t :refer [is are deftest testing async do-report]]
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
      {:keys      (into [] (.-keys ^set/Node data))
       :addresses (into [] (.-_addresses ^set/Node data))}
      {:keys (into [] (.-keys ^set/Leaf data))})))

(defn read-data
  [addr ^js data]
  (case addr
    "tail" data
    "root" data
    (let [keys (into-array (:keys data))]
      (if-some [addresses (:addresses data)]
        (set/Node. keys (arrays/make-array (count addresses)) (into-array addresses))
        (set/Leaf. keys)))))


(defrecord Storage [*disk *reads *writes *deletes]
  storage/IStorage
  (-accessed [_ _addr])
  (-restore [_ addr]
    (when *reads
      (vswap! *reads conj addr))
    (read-data addr (get @*disk addr)))

  (-store [_ addr+nodes]
    (let [addr+nodes (mapv
                      (fn [[addr node]]
                        [(if (nil? addr)
                           (str (random-uuid))
                           addr)
                         node])
                      addr+nodes)]
      (doseq [[addr data] addr+nodes]
        (vswap! *disk assoc addr (write-data addr data))
        (when *writes
          (vswap! *writes conj addr)))
      (mapv first addr+nodes)))
  (-list-addresses [_]
    (keys @*disk))
  (-delete [_ addrs-seq]
    (doseq [addr addrs-seq]
      (vswap! *disk dissoc addr)
      (when *deletes
        (vswap! *deletes conj addr)))))

(defrecord AsyncStorage [*disk *reads *writes *deletes]
  storage/IStorage
  (-accessed [_ _addr])
  (-restore [_ addr]
    (p/do!
     (when *reads
       (vswap! *reads conj addr))
     (read-data addr (get @*disk addr))))

  (-store [_ addr+nodes]
    (p/do!
     (let [addr+nodes (mapv
                       (fn [[addr node]]
                         [(if (nil? addr)
                            (str (random-uuid))
                            addr)
                          node])
                       addr+nodes)]
       (doseq [[addr data] addr+nodes]
         (vswap! *disk assoc addr (write-data addr data))
         (when *writes
           (vswap! *writes conj addr)))
       (mapv first addr+nodes))))
  (-list-addresses [_]
    (p/do!
     (keys @*disk)))
  (-delete [_ addrs-seq]
    (p/do!
     (doseq [addr addrs-seq]
       (vswap! *disk dissoc addr)
       (when *deletes
         (vswap! *deletes conj addr))))))

(defn make-storage [& [opts]]
  (map->Storage
   {:*disk    (volatile! {})
    :*reads   (when (:stats opts)
                (volatile! []))
    :*writes  (when (:stats opts)
                (volatile! []))
    :*deletes (when (:stats opts)
                (volatile! []))}))

(defn make-async-storage [& [opts]]
  (map->AsyncStorage
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
   (d/empty-db nil (merge {:branching-factor 1024, :ref-type :strong :store-group-size 5} opts))
   (map #(vector :db/add % :str (str %)) (range 1 4001))))

(deftest test-async-storage
  (async done
    (-> (p/let [db      (large-db)
                storage (make-async-storage {:stats true})]
          (testing "store"
            (p/do!
             (d/store db storage)
             (is (= 19 (count @(:*writes storage))))  ;; root, tail, avet root + 8 * 2 indexes

             (d/store db)
             (is (= 19 (count @(:*writes storage)))))) ;; store nothing if nothing changed

          (testing "restore"
            (p/let [db' (d/restore storage)]
              (is (= 5 (count @(:*reads storage)))) ;; read root + tail + root idxs

              (p/let [datoms (d/datoms db' :eavt 1)]
                (is (= [1 :str "1"] (-> datoms first ((juxt :e :a :v)))))
                (is (= 6 (count @(:*reads storage))))) ;; read 1 leaf

              (p/let [datoms (d/datoms db' :eavt)]
                (first datoms)
                (is (= 12 (count @(:*reads storage))))) ;; read all leaves

              (p/let [datoms (d/datoms db' :eavt)]
                (vec datoms)
                (is (= 12 (count @(:*reads storage))))) ;; second time no read

              (p/let [eavt (seq (:eavt db))
                      aevt (seq (:aevt db))
                      avet (seq (:avet db))
                      eavt' (seq (:eavt db'))
                      aevt' (seq (:aevt db'))
                      avet' (seq (:avet db'))]
                (is (= db db'))
                (is (= eavt eavt'))
                (is (= aevt aevt'))
                (is (= avet avet')))

              (p/let [db (d/restore storage)]
                (p/let [r (d/q
                           '[:find ?e
                             :where
                             [?e :str _]
                             [(< 1000 ?e 2001)]]
                           db)]
                  (is (= 1000 (count r))))

                (p/let [p (d/pull db '[*] 1024)]
                  (is (= "1024" (:str p)))))))

          (testing "conn"
            (p/let [conn (d/restore-conn storage)]
              (d/transact! conn [[:db/add 1 :name "Ivan"]])
              (is (= 20 (count @(:*writes storage))))
              (is (= @#'storage/tail-addr (last @(:*writes storage))))
              
              ;; only writing tail
              (d/transact! conn [[:db/add 2 :name "Oleg"]])
              (is (= 21 (count @(:*writes storage))))
              (is (= @#'storage/tail-addr (last @(:*writes storage))))
              (is (= 2 (count (:tx-tail @(:atom conn)))))
              (is (= 2 (count (apply concat (:tx-tail @(:atom conn))))))
              
              ;; bigger tx, still writing tail
              (d/transact! conn (mapv #(vector :db/add % :name (str %)) (range 3 1025)))
              (is (= 22 (count @(:*writes storage))))
              (is (= @#'storage/tail-addr (last @(:*writes storage))))
              (is (= 3 (count (:tx-tail @(:atom conn)))))
              (is (= 1024 (count (apply concat (:tx-tail @(:atom conn))))))
              
              ;; tail overflows, flush db
              (d/transact! conn [[:db/add 1025 :name "Petr"]])
              (is (= 36 (count @(:*writes storage)))))))
        (p/then #(done))
        (p/catch (fn [e]
                   ;; (js/console.error e)
                   (do-report {:type     :error
                               :message  "Async error occurred"
                               :expected "No error"
                               :actual   e})
                   (done))))))

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
            (is (map? (d/settings db'))))))))

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
          (is (= 8 (count @(:*writes storage))))))) ;; root, tail + 2 leaves * 2 indexes
    ))

(deftest test-gc
  (let [storage (make-storage {:stats true})]
    (let [db (large-db {:storage storage})]
      (d/store db)
      (is (= 19 (count (d/addresses db))))
      (is (= 19 (count (storage/-list-addresses storage))))
      (is (= (d/addresses db) (set (storage/-list-addresses storage))))
      
      (let [db' (d/db-with db [[:db/add 4001 :str "4001"]])]
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
    (storage/fake-system-gc)
    (let [db'' (d/restore storage)]
      (d/collect-garbage storage)
      (is (= (d/addresses db'') (set (storage/-list-addresses storage))))
      (is (= 4 (count @(:*deletes storage)))))
    
    (testing "don’t delete currently stored db"
      (storage/fake-system-gc)
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

        (storage/fake-system-gc)
        ;; we have to pass in the db because in the prev step we cleared it
        (d/collect-garbage storage [(:db-last-stored @(:atom conn''))])
        (is (= (count (storage/-list-addresses storage))
               (count (d/addresses (:db-last-stored @(:atom conn''))))))
        
        (let [conn''' (d/restore-conn storage)]
          (is (= @conn'' @conn''')))))))

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
