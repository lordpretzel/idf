[![License: GPL 3](https://img.shields.io/badge/license-GPL_3-green.svg)](http://www.gnu.org/licenses/gpl-3.0.txt)
<!-- [![GitHub release](https://img.shields.io/github/release/lordpretzel/idf.svg?maxAge=86400)](https://github.com/lordpretzel/idf/releases) -->
<!-- [![MELPA Stable](http://stable.melpa.org/packages/idf-badge.svg)](http://stable.melpa.org/#/idf) -->
<!-- [![MELPA](http://melpa.org/packages/idf-badge.svg)](http://melpa.org/#/idf) -->
[![Build Status](https://secure.travis-ci.org/lordpretzel/idf.png)](http://travis-ci.org/lordpretzel/idf)


# idf

Library for incremental dataflows in emacs. This library let's you construct dataflow graphs, i.e., computations on collections that can be evaluated lazily (on demand) and whose results can be materialized. The materialized versions can then be incrementally maintained, i.e., you provide inserts and deletions for the data sources processed by a dataflow graph and `idf` will update the results of the dataflow based on the

## Example



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

### Quelpa

Using [use-package](https://github.com/jwiegley/use-package) with [quelpa](https://github.com/quelpa/quelpa).

~~~elisp
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
(use-package idf
:straight (idf :type git :host github :repo "lordpretzel/idf")
~~~

### Source

Alternatively, install from source. First, clone the source code:

~~~sh
cd MY-PATH
git clone https://github.com/lordpretzel/idf.git
~~~

Now, from Emacs execute:

~~~
M-x package-install-file RET MY-PATH/idf
~~~

Alternatively to the second step, add this to your Symbol’s value as variable is void: \.emacs file:

~~~elisp
(add-to-list 'load-path "MY-PATH/idf")
(require 'idf)
~~~
