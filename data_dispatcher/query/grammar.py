ProjectQueryGrammar = """
?metadata_expression:   meta_or                                                           

meta_or:    meta_and ( "or" meta_and )*

meta_and:   term_meta ( "and" term_meta )*

?term_meta:  scalar CMPOP constant                  -> cmp_op
    | scalar "in" constant ":" constant             -> in_range
    | scalar "not" "in" constant ":" constant       -> not_in_range
    | scalar "in" "(" constant_list ")"             -> in_set
    | scalar "not" "in" "(" constant_list ")"       -> not_in_set
    | ANAME "present"?                              -> present                   
    | ANAME "not" "present"                         -> not_present                   
    | constant "in" ATTR_NAME                           -> constant_in_attr
    | constant "not" "in" ATTR_NAME                     -> constant_not_in_attr
    | constant "in" META_NAME                           -> constant_in_meta
    | constant "not" "in" META_NAME                     -> constant_not_in_meta
    | "(" metadata_expression ")"                              
    | "!" term_meta                                 -> meta_not

scalar: ATTR_NAME                                       -> object_attribute
        | META_NAME                                     -> meta_attribute
        | META_NAME "[" "all" "]"                       -> array_all
        | META_NAME "[" "any" "]"                       -> array_any
        | META_NAME "[" SIGNED_INT "]"                  -> array_subscript
        | META_NAME "[" STRING "]"                      -> array_subscript
        | "len" "(" META_NAME ")"                       -> array_length

ATTR_NAME: "owner" | "state" | "created" | "ended" | "id" | "query"

META_NAME: WORD                            

constant_list:    constant ("," constant)*                    

constant : SIGNED_FLOAT                             -> float_constant                      
    | STRING                                        -> string_constant
    | SIGNED_INT                                    -> int_constant
    | BOOL                                          -> bool_constant
    | UNQUOTED_STRING                               -> string_constant

index:  STRING
    | SIGNED_INT

ANAME: WORD ("." WORD)*

FNAME: LETTER ("_"|"-"|"."|LETTER|DIGIT|"/")*

FID: ("_"|"-"|"."|LETTER|DIGIT|"/")+

WORD: LETTER ("_"|LETTER|DIGIT)*

CMPOP:  "<" "="? | "!"? "=" "="? | "!"? "~" "*"? | ">" "="? | "like"            //# like is not implemented yet

BOOL: "true"i | "false"i

STRING : /("(?!"").*?(?<!\\\\)(\\\\\\\\)*?"|'(?!'').*?(?<!\\\\)(\\\\\\\\)*?')/i
UNQUOTED_STRING : /[a-z0-9:%$@_^.%*?-]+/i


%import common.CNAME
%import common.SIGNED_INT
%import common.SIGNED_FLOAT

%import common.WS
%import common.LETTER
%import common.DIGIT
%ignore WS
"""
