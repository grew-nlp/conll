exception Error of Yojson.Basic.t

val error :
  ?file:string ->
  ?line:int ->
  ?fct:string ->
  ?data:string ->
  string -> 'a

module type Id_type = sig
  type t = int * int option (* 8.1 --> (8, Some 1) *)

  val to_string: t -> string

  val to_dot: t -> string

  val to_int: t -> int option

  exception Wrong_id of string

  val of_string: string -> t
  val of_int: int -> t

  val compare: t -> t -> int

  (* [min id1 id2] return the smallest id (according to compare) *)
  val min: t -> t -> t

  (* [max id1 id2] return the biggest id (according to compare) *)
  val max: t -> t -> t

  (* [min_max id1 id2] return (min,max) in a signle call *)
  val min_max: t -> t -> (t*t)

  (* [shift delta id] increases the position by delta *)
  val shift: int -> t -> t
end

module Id : Id_type

module Id_set : Set.S with type elt = Id.t
module Id_map : Map.S with type key = Id.t

module Int_map : Map.S with type key = int
