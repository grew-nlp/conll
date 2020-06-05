exception Conllx_error of Yojson.Basic.t

module Conllx_profile : sig
  type t

  val default: t
end

module Conllx_config: sig
  type t

  val sud: t
  val sequoia: t

end

module Conllx : sig
  type t

  val get_meta: t -> (string * string) list

  val of_json: Yojson.Basic.t -> t

  val to_json: t -> Yojson.Basic.t

  val to_string: ?config: Conllx_config.t -> ?profile: Conllx_profile.t -> t -> string

  val of_string: ?config: Conllx_config.t -> ?profile: Conllx_profile.t -> string -> t
end

module Conllx_corpus : sig
  type t

  val load: ?config: Conllx_config.t -> string -> t
  val read: ?config: Conllx_config.t -> unit -> t

  val to_string: ?config: Conllx_config.t -> t -> string

  val get_data: t ->  (string * Conllx.t) array
end