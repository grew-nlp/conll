open Conll

let file = Sys.argv.(1)
let corpus = Conll_corpus.load file

let _ = Printf.printf "%d sentences\n%!"