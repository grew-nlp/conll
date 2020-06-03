exception Conllx_error of Yojson.Basic.t

module Profile : sig
  type t

  val default: t
end

module Conllx : sig
  type t

  val get_meta: t -> (string * string) list

  val from_json: Yojson.Basic.t -> t

  val to_json: t -> Yojson.Basic.t

  val to_string: ?profile: Profile.t -> t -> string

  val from_string: ?profile: Profile.t -> string -> t
end

module Corpusx : sig
  type t

  val load: string -> t

  val to_string: t -> string

  val get_data: t ->  (string * Conllx.t) array
end