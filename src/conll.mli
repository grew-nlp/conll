module Conll : sig

  type line = {
    line_num: int;
    id: int;
    form: string;
    lemma: string;
    upos: string;
    xpos: string;
    feats: (string * string) list;
    deps: (int * string ) list;
  }

  type multiword = {
    first: int;
    last: int;
    fusion: string;
  }

  type t = {
    meta: string list;
    lines: line list;
    multiwords: multiword list;
  }

  (* val parse: string -> (int * string) list -> t *)
  val from_string: string -> t
  val to_string: t -> string
  val load: string -> t
end

module Corpus : sig
  type t = (string * Conll.t) array
  val load: string -> t
  val save: string -> t -> unit
end
