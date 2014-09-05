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
distributionProcess n lst = map (\x -> snd x) (groupByKey (map (\x -> (mod (snd x) n, fst x) ) (zip lst [0..((length lst)-1)])))
-- distributionProcess 3 [1,2,3,4,5,6,7,8,9,10]
-- Que pasa si pido mas computadoras que el largo de la lista? Respetaria el enunciado?

-- Ejercicio 7
mapperProcess :: Eq k => Mapper a k v -> [a] -> [(k,[v])]
mapperProcess f lst = groupByKey (foldr (\x rec -> (f x) ++ rec) [] lst)

-- Ejercicio 8
combinerProcess :: (Eq k, Ord k) => [[(k, [v])]] -> [(k,[v])]
combinerProcess lst = sortBy (\x y -> if (fst x) >= (fst y) then GT else LT) (foldr (\x rec -> unionWith (++) x rec) [] lst)
-- Necesita testing, unionWith no implementada todavía

-- Ejercicio 9
reducerProcess :: Reducer k v b -> [(k, [v])] -> [b]
reducerProcess red = foldr (\x rec -> (red x) ++ rec) []

-- Ejercicio 10
mapReduce :: (Eq k, Ord k) => Mapper a k v -> Reducer k v b -> [a] -> [b]
mapReduce f g lst = reducerProcess g (combinerProcess (map (mapperProcess f) (distributionProcess 100 lst)))
-- Necesita testing!

-- Ejercicio 11
visitasPorMonumento :: [String] -> Dict String Int
visitasPorMonumento xs = mapReduce (mapper) (reducer) xs
  where mapper k = [(k, 1)]
        reducer (k, vs) = [(k, sum vs)]

-- *MapReduce> visitasPorMonumento ["m1", "m2", "m3", "m2"]
-- [("m1",1),("m2",2),("m3",1)]



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