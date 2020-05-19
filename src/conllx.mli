module Profile : sig
  type t

  val default: t
end

module Conllx : sig
  type t

  val from_json: Yojson.Basic.t -> t

  val to_json: t -> Yojson.Basic.t

  val to_string: Profile.t -> t -> string
end

module Corpusx : sig
  type t

  val load: string -> t

  val to_string: t -> string

  val get_data: t ->  (string * Conllx.t) array
end