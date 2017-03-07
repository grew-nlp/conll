module type Id_type = sig
  type t = int * int option (* 8.1 --> (8, Some 1) *)

  val to_string: t -> string

  val to_dot: t -> string

  exception Wrong_id of string

  val of_string: string -> t

end

module Id : Id_type
