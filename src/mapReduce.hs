module MapReduce where

import Data.Ord
import Data.List

-- ---------------------------------Sección 1---------Diccionario ---------------------------
type Dict k v = [(k,v)]

-- Ejercicio 1
-- 'any f xs' devuelve True si alguno de los elementos de la lista xs
-- cumple con f. Usamos como f una función que devuelve True si la primer
-- componente es la clave deseada.
belongs :: Eq k => k -> Dict k v -> Bool
belongs key = any (\x -> (fst x)==key)

-- Sólo necesitamos invertir el orden en que recibe los parámetros.
(?) :: Eq k => Dict k v -> k -> Bool
(?) = flip belongs
--Main> [("calle",[3]),("city",[2,1])] ? "city" 
--True

-- Ejercicio 2
-- La función parámetro del foldr1 decide si la tupla es la indicada o hay que
-- seguir buscando. Se recorre el diccionario en busca de la clave 'k', y al
-- encontrarla se devuelve su definición.
get :: Eq k => k -> Dict k v -> v
get key dict = snd (foldr1 (\e (k,v) -> (if k==key then (k,v) else e)) dict)

-- Nuevamente, invertir el orden de los parámetros es suficiente.
(!) :: Eq k => Dict k v -> k -> v
(!) = flip get
--Main> [("calle",[3]),("city",[2,1])] ! "city" 
--[2,1]

-- Ejercicio 3

insertWith :: Eq k => (v -> v -> v) -> k -> v -> Dict k v -> Dict k v
insertWith f newK newV dict = if (dict ? newK)
                            then map (\(k,v) -> (if k==newK then (k,(f v newV)) else (k,v))) dict
                            else dict++[(newK,newV)]
--Main> insertWith (++) 2 ['p'] (insertWith (++) 1 ['a','b'] (insertWith (++) 1 ['l'] []))
--[(1,"lab"),(2,"p")]

-- Ejercicio 4
groupByKey :: Eq k => [(k,v)] -> Dict k [v]
groupByKey dict = foldl (flip (uncurry (insertWith (++)))) [] (map (\(k,v) -> (k,[v])) dict)

-- Ejercicio 5
unionWith :: Eq k => (v -> v -> v) -> Dict k v -> Dict k v -> Dict k v
unionWith f dict1 = foldr (uncurry (insertWith f)) dict1
--Main> unionWith (++) [("calle",[3]),("city",[2,1])] [("calle", [4]), ("altura", [1,3,2])]
--[("calle",[3,4]),("city",[2,1]),("altura",[1,3,2])]

-- ------------------------------Sección 2--------------MapReduce---------------------------

type Mapper a k v = a -> [(k,v)]
type Reducer k v b = (k, [v]) -> [b]

-- Ejercicio 6
distributionProcess :: Int -> [a] -> [[a]]
distributionProcess n lst = reverse (foldr (\x rec -> rotate ((x : head (rec)) : tail(rec))) (replicate n []) lst)
  where rotate (x:xs) = xs ++ [x]
-- distributionProcess 3 [1,2,3,4,5,6,7,8,9,10]

-- Ejercicio 7
mapperProcess :: Eq k => Mapper a k v -> [a] -> [(k,[v])]
mapperProcess f lst = groupByKey (foldr (\x rec -> (f x) ++ rec) [] lst)

-- Ejercicio 8
combinerProcess :: (Eq k, Ord k) => [[(k, [v])]] -> [(k,[v])]
combinerProcess lst = sortBy (\x y -> if (fst x) >= (fst y) then GT else LT) (foldr (\x rec -> unionWith (++) x rec) [] lst)

-- Ejercicio 9
reducerProcess :: Reducer k v b -> [(k, [v])] -> [b]
reducerProcess red = foldr (\x rec -> (red x) ++ rec) []

-- Ejercicio 10
mapReduce :: (Eq k, Ord k) => Mapper a k v -> Reducer k v b -> [a] -> [b]
mapReduce f g lst = reducerProcess g (combinerProcess (map (mapperProcess f) (distributionProcess 100 lst)))

-- Ejercicio 11
visitasPorMonumento :: [String] -> Dict String Int
visitasPorMonumento = mapReduce mapper reducer
  where mapper k = [(k, 1)]
        reducer (k, vs) = [(k, sum vs)]

-- *MapReduce> visitasPorMonumento ["m1", "m2", "m3", "m2"]
-- [("m1",1),("m2",2),("m3",1)]

-- Ejercicio 12
monumentosTop :: [String] -> [String]
monumentosTop = (mapReduce mapper reducer) . visitasPorMonumento
  where mapper (k, v) = [(-v, k)]
        reducer (k, vs) = vs 

-- *MapReduce> monumentosTop ["m1", "m2", "m3", "m2", "m3", "m3", "m1", "m4"]
-- ["m3","m1","m2","m4"]

-- Ejercicio 13 
monumentosPorPais :: [(Structure, Dict String String)] -> [(String, Int)]
monumentosPorPais = mapReduce mapper reducer
  where mapper (Monument, d) = [(get "country" d, 1)]
        mapper _ = []
        reducer (k, vs) = [(k, sum vs)]

-- *MapReduce> monumentosPorPais items
-- [("Argentina",2),("Irak",1)]

-- ------------------------ Ejemplo de datos del ejercicio 13 ----------------------
data Structure = Street | City | Monument deriving Show

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


------------------------------------------------
------------------------------------------------