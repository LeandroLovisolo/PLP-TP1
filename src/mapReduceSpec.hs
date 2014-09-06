-- Para correr los tests deben cargar en hugs el módulo Tests y evaluar la expresión "main".
-- Algunas funciones que pueden utilizar para chequear resultados:
-- http://hackage.haskell.org/package/hspec-expectations-0.6.1/docs/Test-Hspec-Expectations.html#t:Expectation

import Test.Hspec
import Test.HUnit (assertBool)
import Data.List
import MapReduce

main :: IO ()
main = hspec $ do
  describe "Utilizando Diccionarios" $ do
    it "[Ej. 1] Puede decidirse si un diccionario tiene cierta clave" $ do
      -- n := tamaño del diccionario
      -- n = 0, versión prefija
      belongs "a" []                   `shouldBe` False
      -- n = 1, versión prefija
      belongs "a" [("b", 1)]           `shouldBe` False
      belongs "a" [("a", 1)]           `shouldBe` True
      -- n > 1, versión prefija
      belongs "a" [("b", 1), ("c", 2)] `shouldBe` False
      belongs "a" [("a", 1), ("b", 2)] `shouldBe` True
      -- n = 0, versión infija
      [] ? "a"                         `shouldBe` False
      -- n = 1, versión infija
      [("b", 1)] ? "a"                 `shouldBe` False
      [("a", 1)] ? "a"                 `shouldBe` True
      -- n > 1, versión infija
      [("b", 1), ("c", 2)] ? "a"       `shouldBe` False
      [("a", 1), ("b", 2)] ? "a"       `shouldBe` True

    it "[Ej. 2] Puede extraerse una definición en un diccionario dada su clave" $ do
      -- n := tamaño del diccionario
      -- n = 1, versión prefija
      get "a" [("a", 1)]           `shouldBe` 1
      -- n > 1, versión prefija
      get "a" [("a", 1), ("b", 2)] `shouldBe` 1
      get "b" [("a", 1), ("b", 2)] `shouldBe` 2
      -- n = 1, versión infija
      [("a", 1)] ! "a"             `shouldBe` 1
      -- n > 1, versión infija
      [("a", 1), ("b", 2)] ! "a"   `shouldBe` 1
      [("a", 1), ("b", 2)] ! "b"   `shouldBe` 2

    it "[Ej. 3] Puede reemplazarse una definición en un diccionario dada su clave" $ do
      -- n := tamaño del diccionario
      -- n = 0
      insertWith (++) "a" [1] []                        `shouldBe`        [("a", [1])]
      -- n = 1, clave inexistente
      insertWith (++) "a" [1] [("b", [2])]              `shouldMatchList` [("a", [1]), ("b", [2])]
      -- n = 1, clave existente
      insertWith (++) "a" [2] [("a", [1])]              `shouldBe`        [("a", [1, 2])]
      -- n > 1, clave inexistente
      insertWith (++) "a" [1] [("b", [2]), ("c", [3])]  `shouldMatchList` [("a", [1]), ("b", [2]), ("c", [3])]
      -- n > 1, clave existente
      insertWith (++) "a" [10] [("a", [1]), ("b", [2])] `shouldMatchList` [("a", [1, 10]), ("b", [2])]

    it "[Ej. 4] Puede agruparse una lista de tuplas (k, v) como diccionario Dict k [v]" $ do
      -- n := tamaño del diccionario
      -- n = 0
      groupByKey ([] :: [(String, Int)])        `shouldMatchList`  []
      -- n = 1
      groupByKey [("a", 1)]                     `shouldBe`         [("a", [1])]
      -- n > 1, sin claves repetidas
      groupByKey [("a", 1), ("b", 2)]           `shouldMatchList`  [("a", [1]), ("b", [2])]
      -- n > 1, con claves repetidas
      groupByKey [("a", 1), ("a", 2), ("b", 3)] `shouldMatchOneOf` [[("a", [1, 2]), ("b", [3])],
                                                                    [("a", [2, 1]), ("b", [3])]]

    it "[Ej. 5] Pueden unirse diccionarios agrupando por clave en caso de conflicto" $ do
      -- m := tamaño del primer diccionario, n := tamaño del segundo diccionario
      -- m = n = 0
      unionWith (+) [] []                                     `shouldBe`        ([] :: [(String, Int)])
      -- m = 1, n = 0
      unionWith (+) [("a", 1)] []                             `shouldBe`        [("a", 1)]
      -- m = 0, n = 1
      unionWith (+) [] [("a", 1)]                             `shouldBe`        [("a", 1)]
      -- m = n = 1, sin claves repetidas
      unionWith (+) [("a", 1)] [("b", 2)]                     `shouldMatchList` [("a", 1), ("b", 2)]
      -- m = n = 1, con claves repetidas
      unionWith (+) [("a", 1)] [("a", 2)]                     `shouldBe`        [("a", 3)]
      -- m > 1, n = 1, sin claves repetidas
      unionWith (+) [("a", 1), ("b", 2)] [("c", 3)]           `shouldMatchList` [("a", 1), ("b", 2), ("c", 3)]
      -- m > 1, n = 1, con claves repetidas
      unionWith (+) [("a", 1), ("b", 2)] [("a", 3)]           `shouldMatchList` [("a", 4), ("b", 2)]
      -- m = 1, n > 1, sin claves repetidas
      unionWith (+) [("a", 1)] [("b", 2), ("c", 3)]           `shouldMatchList` [("a", 1), ("b", 2), ("c", 3)]
      -- m = 1, n > 1, con claves repetidas
      unionWith (+) [("a", 1)] [("a", 2), ("b", 3)]           `shouldMatchList` [("a", 3), ("b", 3)]
      -- m > 1, n > 1, sin claves repetidas
      unionWith (+) [("a", 1), ("b", 2)] [("c", 3), ("d", 4)] `shouldMatchList` [("a", 1), ("b", 2), ("c", 3), ("d", 4)]
      -- m > 1, n > 1, con claves repetidas
      unionWith (+) [("a", 1), ("b", 2)] [("a", 3), ("c", 4)] `shouldMatchList` [("a", 4), ("b", 2), ("c", 4)]

    it "[Ej. 6] Se puede distribuir una lista de manera balanceada" $ do
      -- m := longitud de la lista original, n := cantidad de máquinas
      -- m = 0, n = 1
      let n = 1; xs = ([] :: [Int]) in distributionProcess n xs `shouldSatisfy` correctlyDistributed n xs
      -- m = 1, n = 1
      let n = 1; xs = [1]           in distributionProcess n xs `shouldSatisfy` correctlyDistributed n xs
      -- m > n, n = 1
      let n = 1; xs = [1..10]       in distributionProcess n xs `shouldSatisfy` correctlyDistributed n xs
      -- m > n, n > 1, m divisible por n
      let n = 2; xs = [1..10]       in distributionProcess n xs `shouldSatisfy` correctlyDistributed n xs
      -- m > n, n > 1, m no divisible por n
      let n = 3; xs = [1..10]       in distributionProcess n xs `shouldSatisfy` correctlyDistributed n xs
      -- 1 < m < n, n > 1
      let n = 11; xs = [1..10]      in distributionProcess n xs `shouldSatisfy` correctlyDistributed n xs

    it "[Ej. 7] Se puede aplicar una función mapper a los elementos de una lista" $ do
      -- n := longitud de la lista
      -- n = 0
      mapperProcess mapper []        `shouldBe` []
      -- n = 1, un único par (clave, definición) por item
      mapperProcess mapper [1]       `shouldBe` [("a", [1])]
      -- n = 1, múltiples pares (clave, definición) por item
      mapperProcess mapper [2]       `shouldMatchList` [("a", [1]), ("b", [1])]
      -- n > 1, un único par (clave, definición) por item
      mapperProcess mapper [1, 3, 5] `shouldBe` [("a", [1, 1, 1])]
      -- n > 1, múltiples pares (clave, definición) por item
      mapperProcess mapper [1..5]    `shouldMatchList` [("a", [1, 1, 1, 1, 1]), ("b", [1, 1])]

    it "[Ej. 8] Se pueden agrupar y ordenar por clave las salidas de varios mappers" $ do
      -- n := número de mappers usados, m_i := longitud de la salida del i-ésimo mapper
      -- n = 0
      combinerProcess []                                         `shouldBe`      ([] :: [(String, [Int])])
      -- n = 1, m_1 = 1
      combinerProcess [[("a", [1])]]                             `shouldBe`      [("a", [1])]
      -- n = 1, m_1 = 2
      combinerProcess [[("b", [2]), ("a", [1])]]                 `shouldBe`      [("a", [1]), ("b", [2])]
      -- n > 1, m_i = 1 para todo i, sin claves repetidas
      combinerProcess [[("b", [2])], [("a", [1])]]               `shouldBe`      [("a", [1]), ("b", [2])]
      -- n > 1, m_i = 1 para todo i, con claves repetidas
      combinerProcess [[("b", [3])], [("a", [1])], [("a", [2])]] `shouldBeOneOf` [[("a", [1, 2]), ("b", [3])],
                                                                                  [("a", [2, 1]), ("b", [3])]]
      -- n > 1, m_i > 1 para algún i, sin claves repetidas
      combinerProcess [[("c", [3]), ("b", [2])], [("a", [1])]]   `shouldBe`      [("a", [1]), ("b", [2]), ("c", [3])]
      -- n > 1, m_i > 1 para algún i, con claves repetidas
      combinerProcess [[("b", [3]), ("a", [1])], [("a", [2])]]   `shouldBeOneOf` [[("a", [1, 2]), ("b", [3])],
                                                                                  [("a", [2, 1]), ("b", [3])]]

    it "[Ej. 9] Se puede reducir el resultado de combinar la salida de varios mappers" $ do
      -- n := número de pares a reducir, m_i := longitud de la reducción del i-ésimo par
      -- n = 0
      reducerProcess reducer []                                            `shouldBe` []
      -- n = 1, m_1 = 1
      reducerProcess reducer [("a", [1..3])]                               `shouldBe` [6]
      -- n = 1, m_1 > 1
      reducerProcess reducer [("c", [1..3])]                               `shouldBe` [6, 12]
      -- n > 1, m_i = 1 para todo i
      reducerProcess reducer [("a", [1..3]), ("b", [4..6])]                `shouldBe` [6, 15]
      -- n > 1, m_i > 1 para algún i
      reducerProcess reducer [("a", [1..3]), ("b", [4..6]), ("c", [7..9])] `shouldBe` [6, 15, 24, 48]

  describe "Utilizando Map Reduce" $ do
    it "visitas por monumento funciona en algún orden" $ do
      visitasPorMonumento [ "m1" ,"m2" ,"m3" ,"m2","m1", "m3", "m3"] `shouldMatchList` [("m3",3), ("m1",2), ("m2",2)] 

    it "monumentosTop devuelve los más visitados en algún orden" $ do 
      monumentosTop [ "m1", "m0", "m0", "m0", "m2", "m2", "m3"] 
      `shouldSatisfy` (\res -> res == ["m0", "m2", "m3", "m1"] || res == ["m0", "m2", "m1", "m3"])
    it "Distribution process distribuye correctamente los trabajos" $ do
      distributionProcess 4 [1,2,3,4,5,6,7,8,9,10] `shouldMatchList` [[1,5,9],[2,6,10],[3,7],[4,8]]
      distributionProcess 1 [1,2,3,4,5,6,7,8,9,10] `shouldMatchList` [[1,2,3,4,5,6,7,8,9,10]]
      distributionProcess 10 [1,2,3,4,5,6,7,8,9,10] `shouldMatchList` [[1],[2],[3],[4],[5],[6],[7],[8],[9],[10]]
      distributionProcess 12 [1,2,3,4,5,6,7,8,9,10] `shouldMatchList` [[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[],[]]
    it "Mapper process funciona correctamente" $do
      (mapperProcess (\x -> if (mod x 2) == 0 then [("esPar",x)] else [("esImpar",x)]) [1,2,3,4,5,6]) 
        `shouldMatchList` [("esPar",[2,4,6]),("esImpar",[1,3,5])]
      (mapperProcess (\x -> if (mod x 2) == 0 then [("esPar",x)] else [("esImpar",x)]) []) 
        `shouldMatchList` []
    it "Reducer process reduce correctamente" $do
      reducerProcess (\x -> if (fst x) == "esPar" then [length (snd x)] else []) [("esPar",[2,4,6]),("esImpar",[1,3,5])] `shouldMatchList` [3]
      reducerProcess (\x -> if (fst x) == "esPar" then [length (snd x)] else []) [("esImpar",[1,3,5])] `shouldMatchList` []

-- Función auxiliar para testear distributionProcess (ejercicio 6)
correctlyDistributed :: Eq a => Int -> [a] -> [[a]] -> Bool
correctlyDistributed n xs res = sameItems xs (concat res) && correctlyPartitioned
  where correctlyPartitioned  = sort (map length res) == sort correctPartitionSizes
        correctPartitionSizes = [length xs `div` n + (if x < length xs `mod` n then 1 else 0) | x <- [0..n-1]]

-- Mapper auxiliar para testear mapperProcess (ejercicio 7)
mapper :: Mapper Int String Int
mapper x | x `mod` 2 == 0 = [("a", 1), ("b", 1)]
         | otherwise      = [("a", 1)]

-- Reducer auxiliar para testear reducerProcess (ejercicio 9)
reducer :: Reducer String Int Int
reducer (k, vs) | k == "a" || k == "b" = [sum vs]
                | otherwise            = [sum vs, 2 * (sum vs)]

-- Devuelve true si y sólo si ambas listas tienen los mismos elementos
sameItems :: Eq a => [a] -> [a] -> Bool
sameItems xs ys = null (xs \\ ys) && null (ys \\ xs)

-- Verifica que el valor esté incluido en una lista de posibles valores
shouldBeOneOf :: (Show a, Eq a) => a -> [a] -> Expectation
actual `shouldBeOneOf` xs = assertBool ("predicate failed on: " ++ show actual) $ actual `elem` xs

-- Verifica que la lista tenga los mismos elementos que alguna de las listas provistas
shouldMatchOneOf :: (Show a, Eq a) => [a] -> [[a]] -> Expectation
x `shouldMatchOneOf` ys = assertBool ("predicate failed on: " ++ show x) $ any (sameItems x) ys