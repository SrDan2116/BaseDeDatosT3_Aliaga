Aliaga Hurtado, Daniel Alejandro
TRANFORMACIONES:
1. Map:
rdd = sc.parallelize(["Hola", "Mundo", "Spark", "BaseDeDatos"])
rdd_map = rdd.map(lambda palabra: (palabra, len(palabra)))  # Mapea cada palabra con su longitud
print(rdd_map.collect())

2. Filter:
rdd = sc.parallelize([15, 22, 19, 30, 45, 60])
rdd_filter = rdd.filter(lambda x: x > 20)  # Filtra valores mayores a 20
print(rdd_filter.collect())

3. FlatMap:
rdd = sc.parallelize(["manzana platano", "naranja mango"])
rdd_flatMap = rdd.flatMap(lambda x: x.split(" "))  # Divide las frases en palabras individuales
print(rdd_flatMap.collect())

4. Union:
rdd1 = sc.parallelize(["Perro", "Gato"])
rdd2 = sc.parallelize(["Elefante", "Tigre"])
rdd_union = rdd1.union(rdd2)  # Une ambos RDDs en uno solo
print(rdd_union.collect())

5. Intersection:
rdd1 = sc.parallelize(["Daniel", "Miguel", "Jean Piere", "Salomé", "Christian"])
rdd2 = sc.parallelize(["Luis", "Jorge", "Percy", "Daniel", "Salomé"])
rdd_intersection = rdd1.intersection(rdd2)  # Encuentra elementos comunes
print(rdd_intersection.collect())

6. Distinct:
rdd = sc.parallelize(["rojo", "azul", "rojo", "verde", "azul", "amarillo"])
rdd_distinct = rdd.distinct()  # Elimina duplicados
print(rdd_distinct.collect())

7. GroupByKey:
rdd = sc.parallelize([("fruta", "manzana"), ("fruta", "naranja"), ("verdura", "zanahoria"), ("fruta", "uva")])
rdd_groupByKey = rdd.groupByKey().mapValues(list)  # Agrupa valores por clave
print(rdd_groupByKey.collect())

8. ReduceByKey:
rdd = sc.parallelize([("a", 2), ("b", 3), ("a", 1), ("b", 4)])
rdd_reduceByKey = rdd.reduceByKey(lambda x, y: x * y)  # Multiplica valores por clave
print(rdd_reduceByKey.collect())

9. SortByKey:
rdd = sc.parallelize([(3, "tres"), (1, "uno"), (2, "dos")])
rdd_sortByKey = rdd.sortByKey()  # Ordena por clave
print(rdd_sortByKey.collect())

10. Join:
rdd1 = sc.parallelize([("id1", "Daniel"), ("id2", "Salomé")])
rdd2 = sc.parallelize([("id1", 22), ("id2", 21)])
rdd_join = rdd1.join(rdd2)  # Junta por clave
print(rdd_join.collect())

11. Cogroup:
rdd1 = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])
rdd2 = sc.parallelize([("a", "letra A"), ("b", "letra B")])
rdd_cogroup = rdd1.cogroup(rdd2).mapValues(lambda x: (list(x[0]), list(x[1])))
print(rdd_cogroup.collect())

12. Coalesce:
rdd = sc.parallelize(range(1, 50), 10)
rdd_coalesce = rdd.coalesce(5)  # Reduce a 5 particiones
print(rdd_coalesce.glom().collect())

ACCIONES:
1. Reduce:
rdd = sc.parallelize([5, 10, 15, 20])
resultado_reduce = rdd.reduce(lambda x, y: x * y)  # Producto total
print(resultado_reduce)
   
2. Collect:
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
resultado_collect = rdd.filter(lambda x: x % 2 == 0).collect()
print(resultado_collect)
  
3. Count:
rdd = sc.parallelize([1, 6, 3, 7, 10, 2])
resultado_count = rdd.filter(lambda x: x > 5).count()
print(resultado_count) 

4. First:
rdd = sc.parallelize(["primero", "segundo", "tercero"])
resultado_first = rdd.map(lambda x: x.upper()).first()
print(resultado_first)
   
5. Take:
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8])
resultado_take = rdd.filter(lambda x: x % 2 != 0).take(3)
print(resultado_take)  
    
6. SaveAsTextFile:
rdd = sc.parallelize(["apple", "banana", "cherry", "date"])
resultado_rdd = rdd.map(lambda x: x.upper()).filter(lambda x: "A" in x)
resultado_rdd.saveAsTextFile("/ruta/archivo_salida")

7. Max y Min:
rdd = sc.parallelize([12, 7, 15, 3, 10])
resultado_max = rdd.filter(lambda x: x > 5).max()
resultado_min = rdd.filter(lambda x: x > 5).min()
print(resultado_max)  
print(resultado_min)  
    
8. CountByKey:
rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 2), ("b", 3), ("c", 1), ("a", 3)])
resultado_countByKey = rdd.filter(lambda x: x[1] > 1).countByKey()
print(dict(resultado_countByKey)) 
    
9. Foreach:
rdd = sc.parallelize(["apple", "banana", "cherry"])
rdd.map(lambda x: x.upper()).foreach(lambda x: print(f"Procesado: {x}"))

