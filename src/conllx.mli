exception Conllx_error of Yojson.Basic.t

module Conllx_columns : sig
  type t

  (* # global.columns = ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS MISC *)
  val default: t

  (* # global.columns = ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS PARSEME:MWE *)
  val cupt: t

  (* # global.columns = ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS MISC ORFEO:START ORFEO:STOP ORFEO:SPEAKER *)
  val orfeo: t

  (* [build] from a string like "ID FORM UPOS"*)
  val build: string -> t
end

module Conllx_config: sig
  type t

  val default: t

  (** [build] from a constant value. Known values are: "sequoia", "ud", "sud", "orfeo" *)
  val build: string -> t
end

module Conllx : sig
  type t

  val get_meta: t -> (string * string) list

  val of_json: Yojson.Basic.t -> t

  val to_json: t -> Yojson.Basic.t

  val to_string: ?config: Conllx_config.t -> ?columns: Conllx_columns.t -> t -> string

  val of_string: ?config: Conllx_config.t -> ?columns: Conllx_columns.t -> string -> t
end

module Conllx_corpus : sig
  type t

  val load: ?config: Conllx_config.t -> ?columns: Conllx_columns.t -> string -> t

  val load_list: ?config: Conllx_config.t -> ?columns: Conllx_columns.t -> string list -> t

  val read: ?config: Conllx_config.t -> ?columns: Conllx_columns.t -> unit -> t

  val to_string: ?config: Conllx_config.t -> ?columns: Conllx_columns.t -> t -> string

  val get_data: t -> (string * Conllx.t) array

  val sizes: t -> (int * int)  (* number of graphs, number of nodes *)
end


module Conllx_stat : sig
  type t

  type key = Upos | Xpos

  val build: ?config: Conllx_config.t -> key -> Conllx_corpus.t -> t

  val dump: t -> unit

  (* build the table file. Args: corpus_id stat *)
  val to_html: string -> t -> string
end