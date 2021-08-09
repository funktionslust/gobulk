# Coding guidelines

- Order of imports: first go standart library, second internal, third external dependencies, groups are separated by an emtpy line
- Order of structs and struct-funcs: first define struct, second define funcs of struct, third define next struct
- Order of funcs: If A calls B, place A before B
- use `if !valid {` instead of `if valid == false {`
