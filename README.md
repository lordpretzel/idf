[![License: GPL 3](https://img.shields.io/badge/license-GPL_3-green.svg)](http://www.gnu.org/licenses/gpl-3.0.txt)
<!-- [![GitHub release](https://img.shields.io/github/release/lordpretzel/idf.svg?maxAge=86400)](https://github.com/lordpretzel/idf/releases) -->
<!-- [![MELPA Stable](http://stable.melpa.org/packages/idf-badge.svg)](http://stable.melpa.org/#/idf) -->
<!-- [![MELPA](http://melpa.org/packages/idf-badge.svg)](http://melpa.org/#/idf) -->
[![Build Status](https://secure.travis-ci.org/lordpretzel/idf.png)](http://travis-ci.org/lordpretzel/idf)


# idf

Library for incremental dataflows in emacs. This library let's you construct dataflow graphs called `data frames`, i.e., computations on collections that can be evaluated lazily (on demand) and whose results can be materialized. The materialized versions can then be incrementally maintained, i.e., you provide inserts and deletions for the data sources processed by a dataflow graph and `idf` will update the results of the dataflow based on the changes to the input data sources (what elements got inserted and deleted from the data sources).

The rationale is that is you have some large sorted list or hashtable that is the result of a non-trivial computation, it is faster to incrementally maintain the datastructure using `idf` if there are only small changes to the input of the computation than to recompute the full datastructure on every input change.

## Data model

While lists of other datatypes are supported to some degree, the model used in most parts of the library is that a data frame's result is a list of plists which each have the same *"schema"*, i.e., have the same keys. Basically, this is a relational data model. For example,

```elisp
'((:x 1 :y 3)
  (:x 2 :y 3)
  (:x 1 :y 10))
```

Final results of a dataflow graph can be materialized as either sorted lists of plists (using a user provided sort predicate) or hashtables.

## Usage Example

~~~elisp
(require 'idf)

;; create a datasource (a list of plists). In this case from a given list.
(setq mysource (idf-create-source
                :name :seqlist
                :content (--map `(:x ,it :y ,(/ it 1000)) (number-sequence 1 10000))
                :schema '(:x :y)))

;; now let's filter out some plists and then aggregate grouping by :y and materialize the result
(setq myresult
      (idf-materialize-as
       (idf-aggregate
        (idf-filter mysource
                    :expr '(and (> $x 5) (< $x 9950)))
        :group-by '(:y)
        :aggs '((sum :x))
        :schema '(:y :sx))
       :viewtype 'sortedlist
       :sortkeys '(:y)))

;; or if you prefer threading:
(require 'dash)
(setq myresult (-> mysource
                   (idf-filter  :expr '(and (> $x 5) (< $x 9950)))
                   (idf-aggregate
                    :group-by '(:y)
                    :aggs '((sum :x))
                    :schema '(:y :sx))
                   (idf-materialize-as
                    :viewtype 'sortedlist
                    :sortkeys '(:y))))

(idf-mv-get-result myresult)
;; the result is this list: ((:y 0 :sx 499485) (:y 1 :sx 1499500) (:y 2 :sx 2499500) (:y 3 :sx 3499500) (:y 4 :sx 4499500) (:y 5 :sx 5499500) (:y 6 :sx 6499500) (:y 7 :sx 7499500) (:y 8 :sx 8499500) (:y 9 :sx 9000775))

;; maintain the result when :seqlist is updated (by inserting plists)
(idf-maintain myresult
              `(:seqlist ,(idf-delta-create
                           :inserted '((:x 1000 :y 8)
                                       (:x 1000 :y 10)))))
;; this returns a delta for the result: #s(idf-delta ((:y 10 :sx 1000) (:y 8 :sx 8500500)) ((:y 8 :sx 8499500)))

(idf-mv-get-result myresult)
;; the updated results: ((:y 0 :sx 499485) (:y 1 :sx 1499500) (:y 2 :sx 2499500) (:y 3 :sx 3499500) (:y 4 :sx 4499500) (:y 5 :sx 5499500) (:y 6 :sx 6499500) (:y 7 :sx 7499500) (:y 8 :sx 8500500) (:y 9 :sx 9000775) (:y 10 :sx 1000))

;; we also deal with deletions
(idf-maintain myresult
              `(:seqlist ,(idf-delta-create
                           :deleted '((:x 900 :y 0)
                                      (:x 1000 :y 10)))))
;; the result delta: #s(idf-delta ((:y 0 :sx 498585)) ((:y 10 :sx 1000) (:y 0 :sx 499485)))

(idf-mv-get-result myresult)
;; the updated result: ((:y 0 :sx 498585) (:y 1 :sx 1499500) (:y 2 :sx 2499500) (:y 3 :sx 3499500) (:y 4 :sx 4499500) (:y 5 :sx 5499500) (:y 6 :sx 6499500) (:y 7 :sx 7499500) (:y 8 :sx 8500500) (:y 9 :sx 9000775))
~~~

## Example Application

A practical example for which I am using this library is for maintaining is list of contacts for email completion in `mu4e` that contains all contacts that mu4e provides and all contacts from `bbdb`. `mu4e` updates its contact list after whenever it reindexes its database. I am using `idf` to efficiently maintain this a list of contacts for completion.

## Installation

<!-- ### MELPA -->

<!-- Symbol’s value as variable is void: $1 is available from MELPA (both -->
<!-- [stable](http://stable.melpa.org/#/idf) and -->
<!-- [unstable](http://melpa.org/#/idf)).  Assuming your -->
<!-- ((melpa . https://melpa.org/packages/) (gnu . http://elpa.gnu.org/packages/) (org . http://orgmode.org/elpa/)) lists MELPA, just type -->

<!-- ~~~sh -->
<!-- M-x package-install RET idf RET -->
<!-- ~~~ -->

<!-- to install it. -->

### Dependencies

This package required my sorted-list library.

### Quelpa

Using [use-package](https://github.com/jwiegley/use-package) with [quelpa](https://github.com/quelpa/quelpa).

~~~elisp
(use-package
:quelpa ((sorted-list
:fetcher github
:repo "lordpretzel/sorted-list.el")
:upgrade t)
)


(use-package
:quelpa ((idf
:fetcher github
:repo "lordpretzel/idf")
:upgrade t)
)
~~~

### straight

Using [use-package](https://github.com/jwiegley/use-package) with [straight.el](https://github.com/raxod502/straight.el)

~~~elisp
(use-package sorted-list.el
:straight (idf :type git :host github :repo "lordpretzel/sorted-list.el")

(use-package idf
:straight (idf :type git :host github :repo "lordpretzel/idf")
~~~

### Source

Alternatively, install from source. First, clone the source code:

~~~sh
cd MY-PATH
git clone https://github.com/lordpretzel/sorted-list.el.git
cd MY-PATH
git clone https://github.com/lordpretzel/idf.git
~~~

Now, from Emacs execute:

~~~
M-x package-install-file RET MY-PATH/sorted-list.el
M-x package-install-file RET MY-PATH/idf
~~~

Alternatively to the second step, add this to your Symbol’s value as variable is void: \.emacs file:

~~~elisp
(add-to-list 'load-path "MY-PATH/sorted-list.el")
(add-to-list 'load-path "MY-PATH/idf")
(require 'idf)
~~~
