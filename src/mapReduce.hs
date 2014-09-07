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
-- Si 'k' no existe aún en 'd', se lo agrega sin más.
-- En otro caso se agrega el nuevo valor 'v' a la definición de la clave 'k'
-- usando 'f' como función combinadora.
insertWith :: Eq k => (v -> v -> v) -> k -> v -> Dict k v -> Dict k v
insertWith f k v d | d ? k     = map insert d
                   | otherwise = d ++ [(k, v)]
  where insert (k', v') | k' == k    = (k', f v' v)
                        | otherwise  = (k', v')
--Main> insertWith (++) 2 ['p'] (insertWith (++) 1 ['a','b'] (insertWith (++) 1 ['l'] []))
--[(1,"lab"),(2,"p")]

-- Ejercicio 4
groupByKey :: Eq k => [(k,v)] -> Dict k [v]
groupByKey xs = foldl insertIntoDict [] xs
  where insertIntoDict d (k, v) = insertWith (++) k [v] d

-- Ejercicio 5
unionWith :: Eq k => (v -> v -> v) -> Dict k v -> Dict k v -> Dict k v
unionWith f d d' = foldr insertIntoDict d d'
  where insertIntoDict (k, v) d'' = insertWith f k v d''

--Main> unionWith (++) [("calle",[3]),("city",[2,1])] [("calle", [4]), ("altura", [1,3,2])]
--[("calle",[3,4]),("city",[2,1]),("altura",[1,3,2])]

-------------------------------------------------------------------------------
-- MapReduce                                                                 --
-------------------------------------------------------------------------------

type Mapper a k v = a -> [(k,v)]
type Reducer k v b = (k, [v]) -> [b]

-- Ejercicio 6
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