--  Para correr los tests deben cargar en hugs el módulo Tests
--  y evaluar la expresión "main".
-- Algunas funciones que pueden utilizar para chequear resultados:
-- http://hackage.haskell.org/package/hspec-expectations-0.6.1/docs/Test-Hspec-Expectations.html#t:Expectation

import Test.Hspec
import MapReduce

main :: IO ()
main = hspec $ do
  describe "Utilizando Diccionarios" $ do
    it "puede determinarse si un elemento es una clave o no" $ do
      belongs 3 [(3, "A"), (0, "R"), (7, "G")]    `shouldBe` True
      belongs "k" []                              `shouldBe` False
      [("H", [1]), ("E", [2]), ("Y", [0])] ? "R"  `shouldBe` False
      [("V", [1]), ("O", [2]), ("S", [0])] ? "V"  `shouldBe` True
      [("k", [1])] ? "k"                          `shouldBe` True
      [("t", [1])] ? "k"                          `shouldBe` False

    it "puede obtenerse el valor correspondiente a una determinada clave" $ do
      get "k" [("k", "valor")]                    `shouldBe` "valor"
      [(1, [1, 2, 3]), (2, [1, 2]), (3, [])] ! 1  `shouldBe` [1, 2, 3]
      [(9000, "A"), (2, "B"), (0, "C")] ! 2       `shouldBe` "B"
      [(1, "valor1"), (0, "valor2"), (42, "valor3"), (3, "valor4")] ! 3 `shouldBe` "valor4"

  describe "Utilizando Map Reduce" $ do
    it "visitas por monumento funciona en algún orden" $ do
      visitasPorMonumento [ "m1" ,"m2" ,"m3" ,"m2","m1", "m3", "m3"] `shouldMatchList` [("m3",3), ("m1",2), ("m2",2)] 

    it "monumentosTop devuelve los más visitados en algún orden" $ do 
      monumentosTop [ "m1", "m0", "m0", "m0", "m2", "m2", "m3"] 
      `shouldSatisfy` (\res -> res == ["m0", "m2", "m3", "m1"] || res == ["m0", "m2", "m1", "m3"])