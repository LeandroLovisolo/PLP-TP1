module MapReduce where

import Data.Ord
import Data.List

-- ---------------------------------Sección 1---------Diccionario ---------------------------
type Dict k v = [(k,v)]

-- Ejercicio 1
belongs :: Eq k => k -> Dict k v -> Bool
belongs key = any (\x -> (fst x)==key)

(?) :: Eq k => Dict k v -> k -> Bool
(?) = flip belongs
--Main> [("calle",[3]),("city",[2,1])] ? "city" 
--True

-- Ejercicio 2
get :: Eq k => k -> Dict k v -> v
get key dict = snd (foldr1 (\e (k,v) -> (if k==key then (k,v) else e)) dict)

(!) :: Eq k => Dict k v -> k -> v
(!) = flip get
--Main> [("calle",[3]),("city",[2,1])] ! "city" 
--[2,1]

-- Ejercicio 3
insertWith :: Eq k => (v -> v -> v) -> k -> v -> Dict k v -> Dict k v
insertWith f key val dict = if (dict ? key)
                            then map (\(k,v) -> (if k==key then (k,(f v val)) else (k,v))) dict
                            else dict++[(key,val)]
--Main> insertWith (++) 2 ['p'] (insertWith (++) 1 ['a','b'] (insertWith (++) 1 ['l'] []))
--[(1,"lab"),(2,"p")]

-- Ejercicio 4
groupByKey :: Eq k => [(k,v)] -> Dict k [v]
groupByKey dict = foldr (uncurry (insertWith (flip (++)))) [] (map (\(k,v) -> (k,[v])) dict)

-- Ejercicio 5
unionWith :: Eq k => (v -> v -> v) -> Dict k v -> Dict k v -> Dict k v
unionWith = undefined
--Main> unionWith (++) [("calle",[3]),("city",[2,1])] [("calle", [4]), ("altura", [1,3,2])]
--[("calle",[3,4]),("city",[2,1]),("altura",[1,3,2])]


-- ------------------------------Sección 2--------------MapReduce---------------------------

type Mapper a k v = a -> [(k,v)]
type Reducer k v b = (k, [v]) -> [b]

-- Ejercicio 6
distributionProcess :: Int -> [a] -> [[a]]
distributionProcess n = undefined

-- Ejercicio 7
mapperProcess :: Eq k => Mapper a k v -> [a] -> [(k,[v])]
mapperProcess f lst = groupByKey (foldr (\x rec -> (f x) ++ rec) [] lst)
--mapperProcess (\x -> if x % 2 == 0 then 1 else 2) [1,2,3,4,5,6]

-- Ejercicio 8
combinerProcess :: (Eq k, Ord k) => [[(k, [v])]] -> [(k,[v])]
combinerProcess = undefined

-- Ejercicio 9
reducerProcess :: Reducer k v b -> [(k, [v])] -> [b]
reducerProcess red = foldr (\x rec -> (red x) ++ rec) []

-- Ejercicio 10
mapReduce :: (Eq k, Ord k) => Mapper a k v -> Reducer k v b -> [a] -> [b]
mapReduce = undefined

-- Ejercicio 11
visitasPorMonumento :: [String] -> Dict String Int
visitasPorMonumento = undefined

-- Ejercicio 12
monumentosTop :: [String] -> [String]
monumentosTop = undefined

-- Ejercicio 13 
monumentosPorPais :: [(Structure, Dict String String)] -> [(String, Int)]
monumentosPorPais = undefined


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