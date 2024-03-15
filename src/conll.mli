exception Conll_error of Yojson.Basic.t

module Conll_columns : sig
  type t

  val to_string: t -> string

  (* # global.columns = ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS MISC *)
  val default: t

  (* # global.columns = ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS PARSEME:MWE *)
  val cupt: t

  (* # global.columns = ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS PARSEME:MWE FRSEMCOR:NOUN *)
  val frsemcor: t

  (* # global.columns = ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS MISC ORFEO:START ORFEO:STOP ORFEO:SPEAKER *)
  val orfeo: t

  (* [build] from a string like "ID FORM UPOS"*)
  val build: string -> t
end

module Conll_config: sig
  type t

  (** [build] from a constant value. Known values are: "basic", "sequoia", "ud", "sud", "orfeo".
      Raises [Error] for unknown value. *)
  val build: string -> t

  val get_name: t -> string

  val remove_from_feats: string -> t -> t

  val of_json: Yojson.Basic.t -> t
end


module Conll_label : sig
  type t

  val of_json: Yojson.Basic.t -> t

  val to_json: t -> Yojson.Basic.t

  (** [to_string ~config t] tries to convert the label to a compact representation [Ok s].
      The "long" representation [Error "f=u,g=v"] is returned if not possible. *)
  val to_string: config: Conll_config.t -> t -> (string, string) result

  (** [of_string ~config t] parse the compact label representation.
      Must not be used with a long representation! *)
  val of_string: config: Conll_config.t -> string -> t
end



module Conll : sig
  type t

  val get_meta: t -> (string * string) list
  val set_meta: string -> string -> t -> t

  val set_sent_id: string -> t -> t
  val get_sent_id_opt: t -> string option

  val of_json: Yojson.Basic.t -> t

  val to_json: t -> Yojson.Basic.t

  val to_string: ?config: Conll_config.t -> ?columns: Conll_columns.t -> t -> string

  val of_string: ?config: Conll_config.t -> ?columns: Conll_columns.t -> string -> t

  val load: ?config: Conll_config.t -> ?columns: Conll_columns.t -> string -> t
end


module Conll_corpus : sig
  type t


  val load: ?config: Conll_config.t -> ?quiet:bool -> ?log_file: string -> ?columns: Conll_columns.t -> string -> t

  val load_list: ?config: Conll_config.t -> ?quiet:bool -> ?log_file: string -> ?columns: Conll_columns.t -> string list -> t

  val save: ?config: Conll_config.t -> ?sent_id_list:(string list) -> out_channel -> t -> unit

  val of_lines: ?config: Conll_config.t -> ?quiet:bool -> ?log_file: string -> ?columns: Conll_columns.t -> ?file: string -> string list -> t

  val to_string: ?config: Conll_config.t -> ?columns: Conll_columns.t -> t -> string

  val get_data: t -> (string * Conll.t) array
  val get_columns: t -> Conll_columns.t

  val sizes: t -> (int * int)  (* number of graphs, number of nodes *)
end


module Conll_stat : sig
  type t

  val build:
    ?config: Conll_config.t ->
    (string * string option) -> (* gov clustering key. Ex: ("upos", None) *)
    (string * string option) -> (* dev clustering key. Ex: ("ExtPos", Some "upos")  *)
    Conll_corpus.t ->
    t

  val dump: t -> unit

  (* build the table file. Args: corpus_id stat *)
  val to_html:
    string ->
    (string * string option) -> (* gov clustering key. Ex: ("upos", None) *)
    (string * string option) -> (* dev clustering key. Ex: ("ExtPos", Some "upos")  *)
    t ->
    string
end