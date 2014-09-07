module MapReduce where

import Data.Ord
import Data.List
import Data.Function (on)

-------------------------------------------------------------------------------
-- Diccionario                                                               --
-------------------------------------------------------------------------------

type Dict k v = [(k,v)]

-- Ejercicio 1
-- Verifica si un diccionario contiene una definición para una clave dada.
-- La función 'any' toma un predicado y una lista, y devuelve True si algún
-- elemento en la lista satisface el predicado. El predicado utilizado toma
-- una tupla y se satisface si la clave de la tupla coincide con la recibida.
--
-- Ejemplo:
-- *MapReduce> belongs "a" [("a", 1), ("b", 2)]
-- True
belongs :: Eq k => k -> Dict k v -> Bool
belongs k = any ((== k) . fst)

-- Evalúa la función anterior invirtiendo el orden de sus parámetros.
--
-- Ejemplo:
-- *MapReduce> [("a", 1), ("b", 2)] ? "a"
-- True
(?) :: Eq k => Dict k v -> k -> Bool
(?) = flip belongs

-- Ejercicio 2
-- Devuelve la definición asociada a una clave dada (asumiendo que está presente.)
-- La función 'filter' devuelve todos los elementos de una lista que satisfacen un
-- predicado dado. El predicado utilizado toma una tupla y se satisface si la
-- clave de la tupla coincide con la clave recibida. Luego la función 'head' extrae
-- el primer elemento de la lista resultante (que debería ser único ya que no
-- deberían haber claves repetidas) y finalmente 'snd' devuelve la definición de
-- esa tupla. 
--
-- Ejemplo:
-- *MapReduce> get "a" [("a", 1), ("b", 2)]
-- 1
get :: Eq k => k -> Dict k v -> v
get k = snd . head . filter ((== k) . fst)

-- Evalúa la función anterior invirtiendo el orden de sus parámetros.
--
-- Ejemplo:
-- *MapReduce> [("a", 1), ("b", 2)] ! "a"
-- 1
(!) :: Eq k => Dict k v -> k -> v
(!) = flip get

-- Ejercicio 3
-- Inserta una tupla (k, v) a un diccionario si éste no contiene una definición
-- para k. En caso de existir una definición v' para la misma clave, la reemplaza
-- por el resultado de evaluar a f en v y v'.
--
-- Ejemplos:
-- *MapReduce> insertWith (++) "a" [1] [("b", [2]), ("c", [3])]
-- [("a",[1]),("b",[2]),("c",[3])]
-- *MapReduce> insertWith (++) "b" [3] [("a", [1]), ("b", [2])]
-- [("a",[1]),("b",[2,3])]
insertWith :: Eq k => (v -> v -> v) -> k -> v -> Dict k v -> Dict k v
insertWith f k v d | d ? k     = map insert d
                   | otherwise = (k, v):d
  where insert (k', v') | k' == k    = (k', f v' v)
                        | otherwise  = (k', v')

-- Ejercicio 4
-- Dada una lista de tuplas (k, v) devuelve un diccionario de tuplas (k, [v])
-- agrupando en listas las definiciones de las tuplas con misma clave. Utiliza
-- el esquema 'foldl', partiendo de un diccionario vacío al que en cada paso le
-- inserta una tupla (k, [v]) utilizando la función 'insertWith', de manera tal
-- que si dos tuplas con misma clave (k, v) y (k, v') aparecen en ese orden en
-- la lista original, éstas resultarán en (k, [v, v']) en el diccionario final.
-- Por último, se revierte el orden del diccionario resultante con 'reverse'
-- para respetar el orden de la lista original.
--
-- Ejemplo:
-- *MapReduce> groupByKey [("a", 1), ("b", 2)]
-- [("a",[1]),("b",[2])]
-- *MapReduce> groupByKey [("a", 1), ("a", 2), ("b", 3)]
-- [("a",[1,2]),("b",[3])]
groupByKey :: Eq k => [(k,v)] -> Dict k [v]
groupByKey = reverse . foldl (\d (k, v) -> insertWith (++) k [v] d) []

-- Ejercicio 5
-- Dados dos diccionarios, devuelve un nuevo diccionario con las tuplas de ambos.
-- En caso de haber un par de tuplas (k, v) y (k, v') con la misma clave, unifica
-- las definiciones por medio de una función 'f' recibida por parámetro e inserta
-- (k, (f v v')) en el diccionario resultante. Utiliza el esquema de recursión
-- 'foldr' partiendo del primer diccionario como caso base e insertándole en cada
-- paso tuplas del segundo diccionario, utilizando 'insertWith' con la función 'f'
-- para unificar definiciones en caso de haber claves repetidas.
--
-- Ejemplos:
-- *MapReduce> unionWith (++) [("a", [1]), ("b", [2])] [("c", [3]), ("d", [4])]
-- [("c",[3]),("d",[4]),("a",[1]),("b",[2])]
-- *MapReduce> unionWith (++) [("a", [1]), ("b", [2])] [("a", [3]), ("c", [4])]
-- [("c",[4]),("a",[1,3]),("b",[2])]
unionWith :: Eq k => (v -> v -> v) -> Dict k v -> Dict k v -> Dict k v
unionWith f = foldr (\(k, v) -> insertWith f k v)

-------------------------------------------------------------------------------
-- MapReduce                                                                 --
-------------------------------------------------------------------------------

type Mapper a k v = a -> [(k,v)]
type Reducer k v b = (k, [v]) -> [b]

-- Ejercicio 6
-- Distribuye los elementos de una lista entre 'n' listas de manera balanceada.
-- Utiliza el esquema de recursión 'foldr' partiendo de 'n' listas vacías. En
-- cada paso toma la primer lista, le inserta un elemento de la lista original
-- y rota las 'n' listas antes de continuar. Finalmente revierte el orden de
-- las 'n' listas para volver a su orden original.
--
-- Ejemplo:
-- *MapReduce> distributionProcess 5 [1..12]
-- [[1,6,11],[2,7,12],[3,8],[4,9],[5,10]]
distributionProcess :: Int -> [a] -> [[a]]
distributionProcess n = reverse . foldr addToFirstAndRotate (replicate n [])
  where addToFirstAndRotate x (y:ys) = rotate ((x:y):ys)
        rotate (x:xs)                = xs ++ [x]

-- Ejercicio 7
mapperProcess :: Eq k => Mapper a k v -> [a] -> [(k,[v])]
mapperProcess f = groupByKey . concat . map f

-- Ejercicio 8
combinerProcess :: (Eq k, Ord k) => [[(k, [v])]] -> [(k,[v])]
combinerProcess = sortBy (comparing fst) . foldr (unionWith (++)) []

-- Ejercicio 9
reducerProcess :: Reducer k v b -> [(k, [v])] -> [b]
reducerProcess f = concat . map f

-- Ejercicio 10
mapReduce :: (Eq k, Ord k) => Mapper a k v -> Reducer k v b -> [a] -> [b]
mapReduce m r = reducerProcess r . combinerProcess . map (mapperProcess m) . distributionProcess 100

-------------------------------------------------------------------------------
-- Utilización                                                               --
-------------------------------------------------------------------------------

-- Ejercicio 11
visitasPorMonumento :: [String] -> Dict String Int
visitasPorMonumento = mapReduce mapper reducer
  where mapper k = [(k, 1)]
        reducer (k, vs) = [(k, sum vs)]

-- *MapReduce> visitasPorMonumento ["m1", "m2", "m3", "m2"]
-- [("m1",1),("m2",2),("m3",1)]

-- Ejercicio 12
monumentosTop :: [String] -> [String]
monumentosTop = mapReduce mapper reducer . visitasPorMonumento
  where mapper (k, v) = [(-v, k)]
        reducer (k, vs) = vs 

-- *MapReduce> monumentosTop ["m1", "m2", "m3", "m2", "m3", "m3", "m1", "m4"]
-- ["m3","m1","m2","m4"]

-- Ejercicio 13 
data Structure = Street | City | Monument deriving Show

monumentosPorPais :: [(Structure, Dict String String)] -> [(String, Int)]
monumentosPorPais = mapReduce mapper reducer
  where mapper (Monument, d) = [(d ! "country", 1)]
        mapper _ = []
        reducer (k, vs) = [(k, sum vs)]

-- *MapReduce> monumentosPorPais items
-- [("Argentina",2),("Irak",1)]

-- Ejercicio 13: entrada de ejemplo
items :: [(Structure, Dict String String)]
items = [
    (Monument, [
      ("name","Obelisco"),
      ("latlong","-36.6033,-57.3817"),
      ("country", "Argentina")]),
    (Street, [
      ("name","Int. Güiraldes"),
      ("latlong","-34.5454,-58.4386"),
      ("country", "Argentina")]),
    (Monument, [
      ("name", "San Martín"),
      ("country", "Argentina"),
      ("latlong", "-34.6033,-58.3817")]),
    (City, [
      ("name", "Paris"),
      ("country", "Francia"),
      ("latlong", "-24.6033,-18.3817")]),
    (Monument, [
      ("name", "Bagdad Bridge"),
      ("country", "Irak"),
      ("new_field", "new"),
      ("latlong", "-11.6033,-12.3817")])
    ]