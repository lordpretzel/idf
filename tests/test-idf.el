(require 'idf)

(ert-deftest test-idf-data-sources ()
  "Test data sources."
  (let ((ds '((1 2 3)
              ("A" "basdasd" "teasdasd")
              ((:a 1 :b 1) (:a 2 :b 2) (:a 2 :b 3)))))
    (dolist (d ds)
      (let ((result (idf-create-source :name "r" :content d)))
        (should (equal d (idf-source-literal-content result)))))))

(ert-deftest test-idf-project ()
  "Test dataframe projection."
  (let ((inout '(;; project a
                 ((:a 1 :b 1) (:a 2 :b 2) (:a 3 :b 4))
                 (:a)
                 ((:a 1) (:a 2) (:a 3))
                 ;; project b
                 ((:a 1 :b 1) (:a 2 :b 2) (:a 3 :b 4))
                 (:b)
                 ((:b 1) (:b 2) (:b 4)))))
    (cl-loop for (in project expected) on inout by #'cdddr
             do
             (let ((pdf (idf-project
                         (idf-create-source :content in)
                         :attrs project)))
               (should (equal (idf-collect pdf)
                              expected))))))

(ert-deftest test-idf-filter ()
  "Test filtering data frames."
  (let ((inout '(;; a < 3
                 ((:a 1 :b 1) (:a 2 :b 2) (:a 3 :b 4))
                 (< $a 3)
                 ((:a 1 :b 1) (:a 2 :b 2))
                 ;; a < 3 AND b > 1
                 ((:a 1 :b 1) (:a 2 :b 2) (:a 3 :b 4))
                 (and (< $a 3) (> $b 1))
                 ((:a 2 :b 2)))))
    (cl-loop for (in pred expected) on inout by #'cdddr
             do
             (let ((pdf (idf-filter
                         (idf-create-source :content in)
                         :expr pred)))
               (should (equal (idf-collect pdf)
                              expected))))))

(ert-deftest test-idf-aggregation ()
  "Test aggregation."
  (let ((tasks '(;; sum(a) group by b
                 ((:a 1 :b 1) (:a 2 :b 1) (:a 3 :b 1))
                 (:b)
                 ((sum :a))
                 ((:b 1 :sum-a 6)))))
    (cl-loop for (in gb aggs expected) on tasks by #'cddddr
             do
             (let ((df (idf-aggregate
                        (idf-create-source :content in)
                        :group-by gb
                        :aggs aggs)))
               (should (equal (idf-collect df)
                              expected))))))

(ert-deftest test-idf-reduce ()
  "Test aggregation."
  (let ((tasks `(;; cons a group by b
                 ((:a 1 :b 1) (:a 2 :b 1) (:a 3 :b 1))
                 (:b)
                 nil
                 ,(lambda (a b) (cons (plist-get b :a) a))
                 ((1 2 3)))))
    (cl-loop for (in gb initval reducef expected) on tasks
             by (lambda (c)
                  (dotimes (i 5) (setq c (cdr c)))
                  c)
             do
             (let ((df (idf-reduce
                        (idf-create-source :content in)
                        :reducefn reducef
                        :initval initval
                        :groupattrs gb
                        )))
               (should (equal (idf-collect df)
                              expected))))))

(ert-deftest test-idf-materialization-sorted-list ()
  "Test materialization."
  (let* ((s (idf-create-source :name :r :content '((:a 10 :b 1) (:a 20 :b 2) (:a 30 :b 1))))
         (v1 (-> s
                 (idf-project :attrs '(:a))
                 (idf-materialize-as :viewtype 'sortedlist :sortkeys '(:a))))
         (v2 (-> s
                 (idf-filter :expr '(equal $a 10))
                 (idf-materialize-as :viewtype 'sortedlist :sortkeys '(:a :b)))))
    (should (equal '((:a 10) (:a 20) (:a 30))
                   (idf-mv-get-result v1)))
    (should (equal '((:a 10 :b 1))
                   (idf-mv-get-result v2)))))

(ert-deftest test-idf-materialization-hashtable ()
  "Test materialization."
  (let* ((s (idf-create-source :name :r :content '((:a 10 :b 1) (:a 20 :b 2) (:a 30 :b 1))))
         (v1 (-> s
                 (idf-project :attrs '(:a))
                 (idf-materialize-as :viewtype 'hashtable :keyattrs '(:a) :valueattrs nil)))
         (v2 (-> s
                 (idf-filter :expr '(equal $a 10))
                 (idf-materialize-as :viewtype 'hashtable :keyattrs '(:a) :valueattrs '(:b)))))
    (should (ht-equal-p (ht-from-alist '((10 . t) (20 . t) (30 . t)) 'equal)
                   (idf-mv-get-result v1)))
    (should (ht-equal-p (ht-from-alist '((10 . 1)) 'equal)
                   (idf-mv-get-result v2)))))

(ert-deftest test-idf-nested-materialized-views ()
  "Test materialized view with a materialized view as a subquery."
  (let* ((s (idf-create-source :name :r :content '((:a 10 :b 1) (:a 20 :b 2) (:a 30 :b 1))))
         (v1 (-> s
              (idf-project :attrs '(:a))
              (idf-materialize-as :viewtype 'sortedlist :sortkeys '(:a))))
         (v2 (-> v1
                 (idf-filter :expr '(equal $a 10))
                 (idf-materialize-as :viewtype 'sortedlist :sortkeys '(:a)))))
    (should (equal '((:a 10) (:a 20) (:a 30))
                   (idf-mv-get-result v1)))
    (should (equal '((:a 10))
                   (idf-mv-get-result v2)))))

(ert-deftest test-idf-maintenance ()
  "Test incremental maintenance."
  (let* ((s (idf-create-source :name :r :content '((:a 10 :b 1) (:a 20 :b 2) (:a 30 :b 1))))
         (v1 (-> s
                 (idf-project :attrs '(:a))
                 (idf-materialize-as :viewtype 'sortedlist :sortkeys '(:a))))
         (v2 (-> s
                 (idf-filter :expr '(equal $a 10))
                 (idf-materialize-as :viewtype 'sortedlist :sortkeys '(:a :b))))
         ;; insertion
         (d1 `(:r ,(idf-delta-create :inserted '((:a 10 :b 50))
                               :deleted nil)))
         ;; deletion
         (d2 `(:r ,(idf-delta-create :inserted nil
                               :deleted '((:a 10 :b 1))))))

    (should (equal (idf-delta-create :inserted '((:a 10))
                                     :deleted nil)
                   (idf-maintain v1 d1)))
    (should (equal (idf-delta-create :inserted nil
                                     :deleted '((:a 10)))
                   (idf-maintain v1 d2)))

    (should (equal (idf-delta-create :inserted '((:a 10 :b 50))
                                     :deleted nil)
                   (idf-maintain v2 d1)))
    (should (equal (idf-delta-create :inserted nil
                                     :deleted '((:a 10 :b 1)))
                   (idf-maintain v2 d2)))))

(ert-deftest test-idf-maintain-aggregate ()
  (let* ((s (idf-create-source :name :r :content '((:a 10 :b 1 :c 10) (:a 20 :b 2 :c 5) (:a 30 :b 1 :c 5))))
         (v (-> s
                (idf-aggregate :group-by nil :aggs '((sum :a)) :schema '(:x))
                (idf-materialize-as :viewtype 'sortedlist
                                    :sortkeys '(:x))))
         (d `(:r ,(idf-delta-create
                   :inserted '((:a 5 :b 10 :c 5))))))
    (should (equal (idf-delta-create
                    :inserted '((:x 65))
                    :deleted '((:x 60)))
                   (idf-maintain v d)))))


(ert-deftest test-idf-maintain-groupby-aggregate ()
  (let* ((s (idf-create-source :name :r :content '((:a 10 :b 1 :c 10) (:a 20 :b 2 :c 5) (:a 30 :b 1 :c 5))))
         (v (-> s
                (idf-aggregate :group-by '(:c) :aggs '((sum :a)) :schema '(:c :x))
                (idf-materialize-as :viewtype 'sortedlist
                                    :sortkeys '(:x :c))))
         (d `(:r ,(idf-delta-create
                   :inserted '((:a 5 :b 10 :c 5)))))
         (result (idf-maintain v d)))
    (should (equal result
                   (idf-delta-create
                    :inserted '((:c 5 :x 55))
                    :deleted '((:c 5 :x 50)))))))

(ert-deftest test-idf-multiple-df-maintenance ()
  "Test incremental maintenance of multiple dataframe that share part of their dataflow graphs."
  (let* ((s (idf-create-source :name :r :content '((:a 10 :b 1 :c 10) (:a 20 :b 2 :c 5) (:a 30 :b 1 :c 5))))
         (v (-> s
                (idf-aggregate :group-by '(:c) :aggs '((sum :a)) :schema '(:c :x))
                (idf-materialize-as :viewtype 'sortedlist
                                    :sortkeys '(:x :c))))
         (v1 (-> v
                 (idf-project :attrs '(:c :x))
                 (idf-materialize-as :viewtype 'sortedlist
                                     :sortkeys '(:c :x))))
         (v2 (-> v
                 (idf-project :attrs '(:x))
                 (idf-materialize-as :viewtype 'sortedlist
                                     :sortkeys '(:x))))
         (d `(:r ,(idf-delta-create
                   :inserted '((:a 5 :b 10 :c 5))))))
    ;; maintain both views
    (idf-maintain-multiple `(,v1 ,v2) d)

    (should (equal (idf-mv-get-result v1)
                   '((:c 10 :x 10)
                     (:c 5 :x 55))))

    (should (equal (idf-mv-get-result v2)
                   '((:x 10)
                     (:x 55))))))
