## 1.7.2 (2020/08/20)
  * Resolve incompatibility with containers 3.0

## 1.7.1 (2020/08/07)
  * Impose containers version less than 3

# 1.7.0 (2020/08/05)
  * Change "__ROOT__" to "__0__" (see https://gitlab.inria.fr/grew/grew_match/issues/21)

## 1.6.12 (2020/08/05)
  * change Corpus_stat to take into account key/subkey

## 1.6.11 (2020/06/25)
  * FRSEMCOR

## 1.6.10 (2020/06/16)
  * more robust handling of corpus (for PARSEME corpora)

## 1.6.9 (2020/06/16)
  * Generalize PARSEME:MWE columns to cover PARSEME data

## 1.6.8 (2020/06/16)
  * Accept feature without value in the MISC column

## 1.6.7 (2020/06/16)
  * Make some evolution on Conllx naming

## 1.6.6 (2020/06/08)
  * put the name of “DEPS” feature in config

## 1.6.5 (2020/06/03)
  * Change Error --> Conllx_error
  * Check consistency at parsing time (edge refers to existing nodes, no duplicate id in nodes)

## 1.6.4 (2020/05/29)
  * Introduction of Conllx (partial)

## 1.6.3 (2020/04/14)
  * Add warning for non-empty white lines

## 1.6.2 (2020/03/10)
  * remove speaker, start and stop feature in written orfeo

## 1.6.1 (2020/02/10)
  * Bug fixes

# 1.6.0 (2020/02/10)
  * add optional arg ~tf_wf which produces the two features textform and wordform at loading time (see https://github.com/UniversalDependencies/docs/issues/683)

## 1.5.7 (2020/01/18)
  * Sort columns and rows in tables

## 1.5.6 (2019/10/16)
  * Improve table display
  * Remove deprecated Pervasives

## 1.5.5 (2019/10/16)
  * Add ocaml version constraint (https://gitlab.inria.fr/grew/grew_doc/issues/6)

## 1.5.4 (2019/09/10)
  * Wrong warning about ids

## 1.5.3 (2019/08/30)
  * log_file optional argument in loading functions

## 1.5.2 (2019/08/23)
  * new html tables

## 1.5.1 (2019/08/22)
  * escape new symbol @ for SUD relations

# 1.5.0 (2019/06/24)
  *  /!\ “Conll.Error” instead of “Conll_types.Error”

## 1.4.1 (2019/03/26)
  * move to opam2

# 1.4.0 (2019/02/10)
  * follow yojson type change
  * add cmxs target

# 1.3.0 (2019/01/10)
  * handle orfeo data (13 column CONLL)

# 1.2.0 (2018/11/22)
  * add projection handling for MWE on multi-postag words

# 1.1.0 (2018/09/21)
  * add new sub-commands: “add_text” and “merge”
  * Fix bug with MISC features

# 1.0.0 (2018/09/10)
  * Add handling of column 11 (Parseme info about MWE/NE)

## 0.17.1 (2018/07/04)
  * improve relation tables

# 0.17.0 (2018/03/13)
  * handle combo fusion+MISC
  * Fix dot output

## 0.16.1 (2018/01/24)
  * fix parsing of feats (failed to load Catalan UD corpus)

# 0.16.0 (2017/12/14)
  * rm dep.ml and dependency to cairo

# 0.15.0 (2017/10/02)
  * add `set_sentid` function

## 0.14.1 (2017/09/07)
  * fix dependency to yojson in opam file

# 0.14.0 (2017/09/05)

  * [**Break**] Change type of `Error` exception from `string` to `Yojson.Basic.json`
  * add `Stat` module (to be used in next version of grew-web)

## 0.13.3 (2017/07/17)
  * fix error with duplicate in feature structures

## 0.13.2 (2017/03/16)
  * fix dependency in opam file

## 0.13.1 (2017/03/16)
  * fix `_tags` file

# 0.13.0 (2017/03/04)
  * add svg output for conll structures
