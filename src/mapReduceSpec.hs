-- Para correr los tests deben cargar en hugs el módulo Tests y evaluar la
-- expresión "main". Algunas funciones que pueden utilizar para chequear resultados:
-- http://hackage.haskell.org/package/hspec-expectations-0.6.1/docs/Test-Hspec-Expectations.html#t:Expectation

import Test.Hspec
import MapReduce

main :: IO ()
main = hspec $ do
  describe "Utilizando Diccionarios" $ do
    it "[Ej. 1] Puede decidirse si un diccionario tiene cierta clave" $ do
      -- Caso base
      belongs "a" []                   `shouldBe` False
      -- Caso n = 1
      belongs "a" [("b", 1)]           `shouldBe` False
      belongs "a" [("a", 1)]           `shouldBe` True
      -- Caso n > 1
      belongs "a" [("b", 1), ("c", 2)] `shouldBe` False
      belongs "a" [("a", 1), ("b", 2)] `shouldBe` True

      -- Caso base
      [] ? "a"                         `shouldBe` False
      -- Caso n = 1
      [("b", 1)] ? "a"                 `shouldBe` False
      [("a", 1)] ? "a"                 `shouldBe` True
      -- Caso n > 1
      [("b", 1), ("c", 2)] ? "a"       `shouldBe` False
      [("a", 1), ("b", 2)] ? "a"       `shouldBe` True

    it "[Ej. 2] Puede extraerse una definición en un diccionario dada su clave" $ do
      -- Caso n = 1
      get "a" [("a", 1)]               `shouldBe` 1
      -- Caso n > 1
      get "a" [("a", 1), ("b", 2)]     `shouldBe` 1
      get "b" [("a", 1), ("b", 2)]     `shouldBe` 2

      -- Caso n = 1
      [("a", 1)] ! "a"                 `shouldBe` 1
      -- Caso n > 1
      [("a", 1), ("b", 2)] ! "a"       `shouldBe` 1
      [("a", 1), ("b", 2)] ! "b"       `shouldBe` 2

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
