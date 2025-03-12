(ns datascript-async.storage
  (:require
   [promesa.core :as p]
   [datascript-async.db :as db]
   [datascript-async.util :as util]
   [me.tonsky.persistent-sorted-set.storage :as set.storage]
   [me.tonsky.persistent-sorted-set-async :as set]))

(defprotocol IStorage
  :extend-via-metadata true
  (-gen-addr [_ node]
    "Generate an address for a given node/leaf. Preferably a string but can be any object")
  (-accessed [_ addr]
    "Called when an address is accessed.")
  (-store [_ addr+data-seq]
    "Gives you a sequence of `[addr data]` pairs to serialize and store.
     `addr`s are the return of -gen-addr.
     `data`s are a node/leaf of the set
     the exception to this is root and tail addrs, those will be clojure serializable structures")
  (-restore [_ addr]
    "Read back and deserialize data stored under single `addr`")
  (-list-addresses [_]
    "Return seq that lists all addresses currently stored in your storage.
     Will be used during GC to remove keys that are no longer used.")
  (-delete [_ addrs-seq]
    "Delete data stored under `addrs` (seq). Will be called during GC"))

(def ^:private root-addr
  "root")

(def ^:private tail-addr
  "tail")

(defn serializable-datom [^Datom d]
  [(.-e d) (.-a d) (.-v d) (.-tx d)])

(deftype StorageAdapter [storage ^:mutable store-buffer]
  set.storage/IStorage
  (store [_ node]
    (let [addr (-gen-addr storage node)]
      (util/log "store" addr)
      (vswap! store-buffer conj! [addr node])
      addr))
  (accessed [_ addr]
    (-accessed storage addr))
  (restore [_ addr]
    (util/log "restore" addr)
    (-restore storage addr)))

(defn make-storage-adapter [storage]
  (StorageAdapter. storage nil))

(defn maybe-adapt-storage [opts]
  (if (:storage opts)
    (update opts :storage make-storage-adapter)
    opts))

(defn storage-adapter ^StorageAdapter [db]
  (when db
    (.-_storage ^set/BTSet (:eavt db))))

(defn storage [db]
  (when-some [adapter (storage-adapter db)]
    (.-storage adapter)))

(def ^:private stored-dbs
  #js [])

(defn- remember-db [db]
  (.push stored-dbs (js/WeakRef. db)))

(defn store-impl! [db ^StorageAdapter adapter force?]
  (remember-db db)
  (let [store-buffer (volatile! (transient []))]
    (set! (.-store-buffer adapter) store-buffer)
    (p/let [eavt-addr (set/store (:eavt db) adapter)
            aevt-addr (set/store (:aevt db) adapter)
            avet-addr (set/store (:avet db) adapter)
            meta (merge
                  {:schema        (:schema db)
                   :max-eid       (:max-eid db)
                   :max-tx        (:max-tx db)
                   :eavt          eavt-addr
                   :aevt          aevt-addr
                   :avet          avet-addr
                   :eavt-metadata (set/set-metadata (:eavt db))
                   :aevt-metadata (set/set-metadata (:aevt db))
                   :avet-metadata (set/set-metadata (:avet db))}
                  (set/settings (:eavt db)))]
      (when (or force? (pos? (count @store-buffer)))
        (vswap! store-buffer conj! [root-addr meta])
        (vswap! store-buffer conj! [tail-addr []])
        (-store (.-storage adapter) (persistent! @store-buffer)))
      (set! (.-store-buffer adapter) nil)
      db)))

(defn store
  ([db]
   (if-some [adapter (storage-adapter db)]
     (store-impl! db adapter false)
     (throw (ex-info "Database has no associated storage" {}))))
  ([db storage]
   (if-some [adapter (storage-adapter db)]
     (let [current-storage (.-storage adapter)]
       (if (identical? current-storage storage)
         (store-impl! db adapter false)
         (throw (ex-info "Database is already stored with another IStorage" {:storage current-storage}))))
     (let [adapter (StorageAdapter. storage nil)]
       (store-impl! db adapter false)))))

(defn store-tail [db tail]
  (-store (storage db) [[tail-addr tail]]))

(defn restore-impl [storage opts]
  (p/let [root (-restore storage root-addr)]
    (when root
      (p/let [tail    (-restore storage tail-addr)
              {:keys [schema eavt aevt avet max-eid max-tx
                      eavt-metadata aevt-metadata avet-metadata]} root
              opts    (merge root opts)
              adapter (make-storage-adapter storage)
              eavt    (set/restore-by db/cmp-datoms-eavt eavt adapter (assoc opts :set-metadata eavt-metadata))
              aevt    (set/restore-by db/cmp-datoms-aevt aevt adapter (assoc opts :set-metadata aevt-metadata))
              avet    (set/restore-by db/cmp-datoms-avet avet adapter (assoc opts :set-metadata avet-metadata))
              db      (db/restore-db
                       {:schema  schema
                        :eavt    eavt
                        :aevt    aevt
                        :avet    avet
                        :max-eid max-eid
                        :max-tx  max-tx})]
        ;; TODO: read all branch nodes
        (set/-root eavt)
        (set/-root aevt)
        (set/-root avet)
        (remember-db db)
        [db tail]))))

(defn db-with-tail [db tail]
  ;; yeah fuck tail for now?
  db
  #_
  (reduce
   (fn [db datoms]
     (if (empty? datoms)
       db
       (as-> db %
         (reduce db/with-datom % datoms)
         (assoc % :max-tx (:tx (first datoms))))))
   db tail))

(defn restore
  ([storage]
   (restore storage {}))
  ([storage opts]
   (p/let [[db tail] (restore-impl storage opts)]
     (db-with-tail db tail))))

(defn- addresses-impl [db visit-fn]
  {:pre [(db/db? db)]}
  (p/do!
   (set/-walk-addresses (:eavt db) visit-fn)
   (set/-walk-addresses (:aevt db) visit-fn)
   (set/-walk-addresses (:avet db) visit-fn)))

(defn addresses [dbs]
  (let [*set     (volatile! (transient #{}))
        visit-fn #(vswap! *set conj! %)]
    (visit-fn root-addr)
    (visit-fn tail-addr)
    (doseq [db dbs]
      (addresses-impl db visit-fn))
    (persistent! @*set)))

;; this doesn't work yet
(defn- read-stored-dbs [storage']
  (let [iter ^Iterator (.iterator stored-dbs)]
    (loop [res (transient [])]
      (if (.hasNext iter)
        (let [ref ^WeakReference (.next iter)
              db  (.get ref)]
          (cond
            (nil? db)
            (do
              (.remove iter)
              (recur res))

            (identical? (storage db) storage')
            (recur (conj! res db))

            :else
            (recur res)))
        (persistent! res)))))

(defn collect-garbage [storage']
  (let [dbs    (conj
                (read-stored-dbs storage')
                (restore storage')) ;; make sure we won’t gc currently stored db
        used   (addresses dbs)
        all    (-list-addresses storage')
        unused (into [] (remove used) all)]
    (util/log "GC: found" (count dbs) "alive db refs," (count used) "used addrs," (count all) "total addrs," (count unused) "unused")
    (-delete storage' unused)))

