open Printf

module type Id_type = sig
  type t = int * int option (* 8.1 --> (8, Some 1) *)

  val to_string: t -> string

  val to_dot: t -> string

  exception Wrong_id of string

  val of_string: string -> t
end

(* ======================================================================================================================== *)
module Id = struct
  type t = int * int option (* 8.1 --> (8, Some 1) *)

  let to_string = function
    | (i, None) -> sprintf "%d" i
    | (i, Some j) -> sprintf "%d.%d" i j

  let to_dot = function
    | (i, None) -> sprintf "%d" i
    | (i, Some j) -> sprintf "%d_%d" i j

  exception Wrong_id of string
  let of_string s =
    try
      match Str.split (Str.regexp "\\.") s with
      | [i] -> (int_of_string i, None)
      | [i;j] -> (int_of_string i, Some (int_of_string j))
      | _ -> raise (Wrong_id s)
    with Failure _ -> raise (Wrong_id s)
end
