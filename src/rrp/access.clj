(ns rrp.access
  "User access managment/sandboxing in datomic"
  (:use [datomic.api :only [q db] :as d]
        clojure.repl
        clojure.pprint
        [clojure.set :as s]))

(def uri "datomic:mem://access")

(d/create-database uri)

(def conn (d/connect uri))

(comment
  A little idea about how to give users (not peers) of your database the power of datomic via sandboxing)

(defn transact-access-schema []
  (let [schema [{:db/id #db/id[:db.part/db]
                 :db/ident :guild/item
                 :db/valueType :db.type/ref
                 :db/cardinality :db.cardinality/many
                 :db/doc "Guilds items"
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id[:db.part/db]
                 :db/ident :item/name
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one
                 :db/doc "Item name"
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id[:db.part/db]
                 :db/ident :item/max-damage
                 :db/valueType :db.type/long
                 :db/cardinality :db.cardinality/one
                 :db/doc "item max-damage property"
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id[:db.part/db]
                 :db/ident :item/min-damage
                 :db/valueType :db.type/long
                 :db/cardinality :db.cardinality/one
                 :db/doc "item min-damage property"
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id[:db.part/db]
                 :db/ident :item/armor
                 :db/valueType :db.type/long
                 :db/cardinality :db.cardinality/one
                 :db/doc "item armor property"
                 :db.install/_attribute :db.part/db}]]
    @(d/transact conn schema)))

(defn tempids [partition]
  (iterate (fn [_] (d/tempid partition)) (d/tempid partition)))

(defn transact-access-data []
  "A query consists of the input variables (saved not exactly elegantly as
   strings) the output variables (dito) and multiple
   clauses (no support for data sources or rules yet)"
  (let [id #(d/tempid :db.part/db)
        [i1id i2id i3id gid gid2] (tempids :db.part/user)
        data [{:db/id gid
               :guild/item [i1id i2id]}
              {:db/id gid2
               :guild/item [i3id]}
              {:db/id i1id
               :item/name "Sword"
               :item/max-damage 10
               :item/min-damage 9}
              {:db/id i2id
               :item/name "Dagger"
               :item/max-damage 7
               :item/min-damage 8}
              {:db/id i3id
               :item/name "Robe"
               :item/armor 19}]]
    @(d/transact conn data)))

(defn transact! []
  (transact-access-schema)
  (transact-access-data))

(defn guilds [db]
  (q {:find '[?g]
      :where '[[?g :guild/item _]]} db))

(def access-rule '[[[access ?g ?e]
                    [?g ?r ?e]
                    [?r :db/valueType :db.type/ref]]
                   [[access ?g ?e]
                    [access ?g ?e2]
                    [access ?e2 ?e]]])

(defn access?
  "Returns all entities a user can access from his sandbox"
  ([user db]
     (let [query {:find '[?e]
                  :in '[$ % ?u]
                  :where '[[access ?u ?e]]}]
       (q query db access-rule user)))
  ([user db entity]
     "Returns all entities a user can access from his sandbox"
     (let [query {:find '[?e]
                  :in '[$ % ?u ?e]
                  :where '[[access ?u ?e]]}]
       (not (empty? (q query db access-rule user entity))))))

(defn user-query [query db user & args]
  "Runs a query in the users sandbox"
  (q query (d/filter db #(access? user %1 (.e %2))) args))

(comment
  Now this is the simplest user sandbox.
  We have to call the filter function for every datum we touch
  But we because we dedicate ourselves to a hierarchical data model we can do better than that.

  '[[?i :item/max-damage ?max]
    [?i :item/min-damage ?e]]
  
  '[[?g :guild/item ?i]
    [access ?g ?i]
    [?i :item/max-damage ?max]
    [?i :item/min-damage ?e]]

  Simple rule
  Only allow to introduce new entity variables which are accessible querying entity)

;;TODO: check that hardcoded '?user variable is not used in query
(defn user-query-fast [{:keys [find in where] :as query} db user & args]
  "Runs a query in the users sandbox"
  (let [graph (into {} (map (fn [[e _ v]]
                              (if (symbol? e) ;;else is a function call
                                [e v])) where))
        roots (s/difference (set (keys graph)) (set (vals graph)))
        extra-clauses (map (fn [symbol] ['access '?user symbol]) roots)
        extra-args '[$ % ?user]
        query (assoc query
                :in (concat extra-args in)
                :where (concat extra-clauses where))] 
    (apply q query db access-rule user args)))

(transact!)
(user-query-fast {:find '[?i] :where '[[?i :item/max-damage ?max] [(> ?max 2)]]} (db conn) (ffirst (guilds (db conn))))

(comment Now the downside to this is that we don't have constraints which could enforce this kind of modelling,
         but it's not hard to imagine such a thing for datomic.
         Another advantage is that if your hierachical model is safe,
         then i guess it is also safe to provide lazy entities via the entity function to a user,
         given that he has acces to the provided entity.)

