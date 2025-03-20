(ns datascript-async.util
  (:refer-clojure :exclude [find])
  (:require
   [promesa.core :as p]
   [me.tonsky.maybe-promise :as mp])
  #?(:clj
     (:import
      [java.util UUID])))

(def ^:dynamic *debug*
  false)

#?(:clj
   (defmacro log [& body]
     (when (System/getProperty "datascript.debug")
       `(when *debug*
          (println ~@body)))))

#?(:clj
   (defmacro raise [& fragments]
     (let [msgs (butlast fragments)
           data (last fragments)]
       `(throw (ex-info (str ~@(map (fn [m#] (if (string? m#) m# (list 'pr-str m#))) msgs)) ~data)))))

#?(:clj
   (def ^:private ^:dynamic *if+-syms))

#?(:clj
   (defn- if+-rewrite-cond-impl [cond]
     (clojure.core/cond
       (empty? cond)
       true
       
       (and
        (= :let (first cond))
        (empty? (second cond)))
       (if+-rewrite-cond-impl (nnext cond))
       
       (= :let (first cond))
       (let [[var val & rest] (second cond)
             sym                (gensym)]
         (vswap! *if+-syms conj [var sym])
         (list 'let [var (list 'clojure.core/vreset! sym val)]
               (if+-rewrite-cond-impl
                   (cons 
                    :let
                    (cons rest
                          (nnext cond))))))
       
       :else
       (list 'and
             (first cond)
             (if+-rewrite-cond-impl (next cond))))))

#?(:clj
   (defn- if+-rewrite-cond [cond]
     (binding [*if+-syms (volatile! [])]
       [(if+-rewrite-cond-impl cond) @*if+-syms])))

#?(:clj
   (defn- flatten-1 [xs]
     (vec
       (mapcat identity xs))))

#?(:clj
   (defmacro if+
     "Allows sharing local variables between condition and then clause.
      
      Use `:let [...]` form (not nested!) inside `and` condition and its bindings
      will be visible in later `and` clauses and inside `then` branch:
      
        (if+ (and
               (= 1 2)
               ;; same :let syntax as in doseq/for
               :let [x 3
                     y (+ x 4)]
               ;; x and y visible downstream
               (> y x))
          
          ;; then: x and y visible here!
          (+ x y 5)
          
          ;; else: no x or y
          6)"
     [cond then else]
     (if (and
           (seq? cond)
           (or
             (= 'and (first cond))
             (= 'clojure.core/and (first cond))))
       (let [[cond' syms] (if+-rewrite-cond (next cond))]
         `(let ~(flatten-1
                  (for [[_ sym] syms]
                    [sym '(volatile! nil)]))
            (if ~cond'
              (let ~(flatten-1
                      (for [[binding sym] syms]
                        [binding (list 'deref sym)]))
                ~then)
              ~else)))
       (list 'if cond then else))))

#?(:clj
   (defmacro cond+ [& clauses]
     (when-some [[test expr & rest] clauses]
       (case test
         :do    `(do ~expr (util/cond+ ~@rest))
         :let   `(let ~expr (util/cond+ ~@rest))
         :mplet `(mp/let ~expr (util/cond+ ~@rest))
         :some  `(or ~expr (util/cond+ ~@rest))
         `(util/if+ ~test ~expr (util/cond+ ~@rest))))))

#?(:clj
   (defmacro some-of
     ([] nil)
     ([x] x)
     ([x & more]
      `(let [x# ~x] (if (nil? x#) (some-of ~@more) x#)))))

(defn- rand-bits [pow]
  (rand-int (bit-shift-left 1 pow)))

#?(:cljs
   (defn- to-hex-string [n l]
     (let [s (.toString n 16)
           c (count s)]
       (cond
         (> c l) (subs s 0 l)
         (< c l) (str (apply str (repeat (- l c) "0")) s)
         :else   s))))

(defn squuid
  ([]
   (squuid #?(:clj  (System/currentTimeMillis)
              :cljs (.getTime (js/Date.)))))
  ([msec]
   #?(:clj
      (let [uuid     (UUID/randomUUID)
            time     (int (/ msec 1000))
            high     (.getMostSignificantBits uuid)
            low      (.getLeastSignificantBits uuid)
            new-high (bit-or (bit-and high 0x00000000FFFFFFFF)
                       (bit-shift-left time 32))]
        (UUID. new-high low))
      :cljs
      (uuid
        (str
          (-> (int (/ msec 1000))
            (to-hex-string 8))
          "-" (-> (rand-bits 16) (to-hex-string 4))
          "-" (-> (rand-bits 16) (bit-and 0x0FFF) (bit-or 0x4000) (to-hex-string 4))
          "-" (-> (rand-bits 16) (bit-and 0x3FFF) (bit-or 0x8000) (to-hex-string 4))
          "-" (-> (rand-bits 16) (to-hex-string 4))
          (-> (rand-bits 16) (to-hex-string 4))
          (-> (rand-bits 16) (to-hex-string 4)))))))

(defn squuid-time-millis
  "Returns time that was used in [[squuid]] call, in milliseconds, rounded to the closest second."
  [uuid]
  #?(:clj (-> (.getMostSignificantBits ^UUID uuid)
            (bit-shift-right 32)
            (* 1000))
     :cljs (-> (subs (str uuid) 0 8)
             (js/parseInt 16)
             (* 1000))))

(defn distinct-by [f coll]
  (->>
    (reduce
      (fn [[seen res :as acc] el]
        (let [key (f el)]
          (if (contains? seen key)
            acc
            [(conj! seen key) (conj! res el)])))
      [(transient #{}) (transient [])]
      coll)
    second
    persistent!))

(defn find [pred xs]
  (reduce
    (fn [_ x]
      (when (pred x)
        (reduced x)))
    nil xs))

(defn single [coll]
  (assert (nil? (next coll)) "Expected single element")
  (first coll))

(defn concatv [& xs]
  (into [] cat xs))

(defn zip
  ([a b]
   (mapv vector a b))
  ([a b & rest]
   (apply mapv vector a b rest)))

(defn removem [key-pred m]
  (persistent!
    (reduce-kv
      (fn [m k v]
        (if (key-pred k)
          m
          (assoc! m k v)))
      (transient (empty m)) m)))

(def conjv
  (fnil conj []))

(def conjs
  (fnil conj #{}))

(defn reduce-indexed
  "Same as reduce, but `f` takes [acc el idx]"
  [f init xs]
  (first
   (reduce
    (fn [[acc idx] x]
      (let [res (f acc x idx)]
        (if (reduced? res)
          (reduced [res idx])
          [res (inc idx)])))
    [init 0]
    xs)))

(defn async-reduce
  "Like reduce but `f` can return a promise"
  ([f coll]
   (if-let [s (seq coll)]
     (async-reduce f (first s) (next s))
     (f)))
  ([f acc coll]
   (reduce
    (fn [acc v]
      (mp/then
       acc
       (fn [acc]
         (f acc v))))
    acc
    coll)))

(defn async-reduce-kv
  "Like reduce-kv but `f` can return a promise"
  [f acc coll]
  (reduce-kv
   (fn [acc k v]
     (mp/then
      acc
      (fn [acc]
        (f acc k v))))
   acc
   coll))

(defn async-mapv
  [f coll]
  (-> (async-reduce
       (fn [acc x]
         (mp/then
          (f x)
          (fn [new-x]
            (conj! acc new-x))))
       (transient [])
       coll)
      (mp/then (fn [ret]
                 (persistent! ret)))))

(defn async-filterv
  [f coll]
  (-> (async-reduce
       (fn [acc x]
         (mp/then
          (f x)
          (fn [keep?]
            (if keep?
              (conj! acc x)
              acc))))
       (transient [])
       coll)
      (mp/then (fn [ret]
                 (persistent! ret)))))

;; An atom mapping each lock to its promise chain.
(defonce async-locks (atom {}))

(defn async-lock*
  "Takes a lock and a thunk (a no-arg function that returns a promise).
   The thunk is chained onto the lock’s promise chain so that
   calls using the same lock run sequentially. Returns a promise
   that resolves with the thunk’s result."
  [lock thunk]
  (let [p-result (p/deferred)]
    (swap! async-locks
           update lock
           (fnil (fn [prev]
                   (let [new-chain (-> prev
                                       (p/then (fn [] (thunk)))
                                       (p/then (fn [res]
                                                 (p/resolve! p-result res)
                                                 res))
                                       (p/catch (fn [err]
                                                  (p/reject! p-result err)
                                                  nil)))]
                     (-> new-chain
                         (p/finally
                           (fn []
                             (swap! async-locks
                                    (fn [locks]
                                      (if (= (get locks lock) new-chain)
                                        (dissoc locks lock)
                                        locks))))))
                     new-chain))
                 (p/resolved nil)))
    p-result))

;; no idea if this works in clojure..
(defmacro async-locking
  "Mimics Clojure's locking macro for async code.
   Usage:
     (async-locking some-lock
       ;; critical section code returning a promise
       ...)"
  [lock & body]
  `(async-lock* ~lock (fn [] ~@body)))
