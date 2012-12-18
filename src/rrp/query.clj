(ns rrp.query
  "Alternative query interface"
  (:use [datomic.api :only [q db] :as d]))

(comment
  Consider a query which depends on user input:
  A user may filter through items using multiple criterias.
  It would be convoluted to write a query for each combination of criterias..
  So i was wondering what would be the best way and if there should be an query-interface builtin for this usecase.)


(comment

  Binding approach

  Make the query take bindigns instead of arguments

  (smart-query [:find ?item
                :where
                [?auction :auction/item ?item]
                [?item :item/price ?price]
                [(< ?price ?maxprice)]
                [(> ?price ?minprice)]
                [?auction :auction/end ?end]
                [(< ?end ?maxend)]
                [?item :item/category ?user-category]
                ] db {`?maxprice 100 `?user-category "Books"})

  Than compile the query down to the normal form, excluding clauses which are not used and
  passing the bindings as arguments.

  (q  '[:find ?item
        :in $ ?maxprice ?user-category
        :where
        [?auction :auction/item ?item]
        [?item :item/price ?price]
        [(< ?price ?maxprice)]
        [?item :item/category ?user-category]] db 100 "Books"))

(comment
  Or alternatively which i guess is somewhat more complicated from the user-perspective
  use a map to choose queries depending on the input.
  Note: Duplicated clauses like [?item :item/price ?price] would still have to be filtered
  depending on the input.

  (qqq '[:find ?item
         :in $ {?maxprice :maxprice ?minprice :minprice }
         :where
         [?auction :auction/item ?item]
         {?maxprice '[[?item :item/price ?price]
                      [(< ?price ?maxprice)]]
          ?minprice '[[?item :item/price ?price]
                      [(> ?price ?minprice)]]
          ?user-category '[[?item :item/category ?category]
                           (= ?category) ?user-category]
          ?maxend       '[[?auction :auction/end ?end]
                          [< ?end ?maxend]]}]
       db {:maxprice 100 :minprice 100}))
