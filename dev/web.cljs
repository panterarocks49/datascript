(ns web
  (:require
   ["idb" :as idb]
   [datascript.transit :as dt]
   [datascript-async.core :as d]
   [datascript-async.db :as ddb]
   [datascript-async.storage :as storage]
   [promesa.core :as p]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]
   [me.tonsky.persistent-sorted-set-async :as pss]))

(defonce idb-db (atom nil))

(defn init-idb []
  (when-not @idb-db
    (-> (idb/openDB "my-database" 1
                    #js {:upgrade (fn [db _old-version _new-version _transaction]
                                    ;; Create an object store named "store"
                                    (.createObjectStore db "store"))})
        (.then (fn [db-instance]
                 (reset! idb-db db-instance)
                 (js/console.log "Database initialized")
                 db-instance))
        (.catch (fn [error]
                  (js/console.error "Error opening database:" error))))))

(init-idb)

(defn print-async
  [f]
  (let [t (.now js/performance)]
    (-> (f)
        (p/catch js/console.error)
        ;; (p/then #_prn)
        (p/finally #(prn (- (.now js/performance) t))))))

(defn read-addr [addr]
  (p/let [x (.get @idb-db "store" addr)]
    (dt/read-transit-str x)))

(defn write-addr+data-seq [data-map]
  (let [tx    (.transaction @idb-db "store" "readwrite" #_ {:durability "relaxed"})
        store (.objectStore tx "store")]
    (p/all
     (mapv
      (fn [[k v]]
        (.put store (dt/write-transit-str v) k))
      data-map))))

;; tdc/transit-write-str

(deftype IDBStorage []
  storage/IStorage
  (-restore [_ addr]
    ;; (prn "RESTORE" addr)
    ;; (read-addr addr)
    (print-async #(read-addr addr))
    )

  (-store [_ addr+data-seq]
    ;; (prn "STORE" (mapv first addr+data-seq))
    (->> addr+data-seq
         (write-addr+data-seq)))
  (-list-addresses [_])
  (-delete [_ addrs-seq]))

(deftype MemoryStorage [*storage]
  storage/IStorage
  (-restore [_ addr]
    (prn "RESTORE" addr)
    (get @*storage addr))

  (-store [_ addr+data-seq]
    (prn "STORE" (mapv first addr+data-seq))
    ;; (prn (first addr+data-seq))
    (swap! *storage into addr+data-seq)
    nil)
  (-list-addresses [_])
  (-delete [_ addrs-seq]))

(defn idb-storage []
  (IDBStorage.)
  #_
  (MemoryStorage. (atom {})))

(def cmp compare #_(compare %2 %1))
(def storage (idb-storage))

(def schema {:children {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}})

(def datoms
  (vec
   (mapcat
    (fn [id]
      [(ddb/datom id :block/string "test this")
       (ddb/datom id :children (dec id))
       (ddb/datom (dec id) :block/open true)])
    (range 2 1000))))

(defonce large-test-datoms
  (let [n 20001]
    (into []
          (mapcat (fn [i]
                    [(ddb/datom i :block/string (str "test " i))
                     (ddb/datom i :block/uid (str "uid" i))
                     (ddb/datom i :block/open true)
                     (ddb/datom i :block/refs 1)
                     (ddb/datom i :block/children (inc i))]))
          (range 1 n))))




(comment
  (def db (time (d/init-db large-test-datoms schema {})))

  (print-async #(d/store db storage))

  (print-async #(d/datoms db :aevt :block/string))


  (p/let [d (d/restore storage)]
    (def restored-db d))

  (print-async #(d/datoms restored-db :aevt :block/string))

  (print-async #(ddb/-datoms restored-db :eavt 2 nil nil nil))

  (print-async #(ddb/-search db [2 :block/string nil nil]))


  (def s (pss/from-sequential
          cmp
          (range 0 10000)
          {:storage storage}))

  (-> (pss/rslice s 1700 1200)
      (p/then (fn [v]
                (prn
                 (reduce
                  conj
                  []
                  v)))))

  (-> (pss/slice s 1200 10700)
      (p/then (fn [v]
                (prn
                 (reduce
                  conj
                  []
                  v)))))

  )






;; start is called by init and after code reloading finishes
(defn ^:dev/after-load start []
  (js/console.log "start"))

(defn init []
  ;; init is called ONCE when the page loads
  ;; this is called in the index.html and must be exported
  ;; so it is available even in :advanced release builds
  (js/console.log "init")
  (start))

;; this is called before any code is reloaded
(defn ^:dev/before-load stop []
  (js/console.log "stop"))


