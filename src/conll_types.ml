open Printf

exception Error of string

let error m = Printf.ksprintf (fun msg -> raise (Error msg)) m

(* ======================================================================================================================== *)
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

  let compare id1 id2 =
    match (id1, id2) with
    | ((i1, _), (i2, _)) when i1 <> i2 -> Pervasives.compare i1 i2
    (* all other cases: i1=i2 *)
    | ((_,None), (_,Some _)) -> -1
    | ((_,Some _), (_,None)) -> 1
    | ((_,Some sub_i1), (_,Some sub_i2)) -> Pervasives.compare sub_i1 sub_i2
    | ((id,None), (_,None)) -> error "[Conll], twice the same indentifier \"%d\"" id

  let min id1 id2 = if compare id1 id2 < 0 then id1 else id2
  let max id1 id2 = if compare id1 id2 < 0 then id2 else id1
  let min_max id1 id2 = if compare id1 id2 < 0 then (id1, id2) else (id2, id1)
end


