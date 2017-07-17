exception Error of string
val error: ('a, unit, string, 'b) format4 -> 'a

module type Id_type = sig
  type t = int * int option (* 8.1 --> (8, Some 1) *)

  val to_string: t -> string

  val to_dot: t -> string

  exception Wrong_id of string

  val of_string: string -> t

  val compare: t -> t -> int

  (* [min id1 id2] return the smallest id (according to compare) *)
  val min: t -> t -> t

  (* [max id1 id2] return the biggest id (according to compare) *)
  val max: t -> t -> t

  (* [min_max id1 id2] return (min,max) in a signle call *)
  val min_max: t -> t -> (t*t)
end

module Id : Id_type