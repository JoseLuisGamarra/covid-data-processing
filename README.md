# Procesamiento en la nube con Azure y Databricks. ICESI 2021-1 - Maestría en Ciencia de Datos

Tutorial para cargar datos desde una fuente Http a un Blob Storage de Azure, para posteriormente ser manipulados desde DataBricks y consumidos con Power Bi. Se pretende configurar todo el flujo de datos desde una fuente a otra utilizando herramientas cloud y al final, poder responder las siguientes preguntas sobre el COVID-19:
1. ¿Cuántos casos existen a nivel global?
1. ¿Cuáles son los paises más afectados?
1. Identificar las regiones o subregiones más críticas en Estados Unidos.
1. ¿Cuál es el índice de mortalidad?


## Objetivos
1. Crear un Blob Storage y su respectivo contenedor en Azure.  **(Almacenamiento)**
1. Crear y configurar la copia de datos desde una fuente Http (alojada en los servidores de Google Cloud) a el Blob Storage. **(ETL)**
1. Crear un punto de montaje al Blob Storage de Azure desde Databricks y realizar las consultas anteriormente propuestas. **(Procesamiento)**
1. Consumir los datos alojados en las tablas de Databricks desde PowerBI. **(Visualización)**


# Crear grupo de recursos
Creamos un grupo de recursos en azure para encapsular los servicios que utilicemos. Para ellos, buscamos el servicios **Resurce groups** y damos clic. 
Seleccionamos la subscripción, damos un nombre al grupo de recursos y seleccionamos la región.

Seleccionamos el grupo de recursos que hemos creado y añadimos un nuevo recurso. En la barra de búsqueda que aparece escribimos Blob y seleccionamos la opción que se llama: Storage account. Nos aparece la siguiente imagen y damos clic en crear.

Seleccionamos la subscripción, el grupo de recursos y escribimos los datos básicos de nuestros Blob Storage, tal como nombre, localización y tipo de almacenamiento, entre otros. Creamos y esperamos que el servicio esté desplegado. Damos clic en go to resource. 

Una vez aquí, damos clic en container y luego en crear. Asignamos un nombre y listo. Lo que hemos hecho hasta aquí es crear el sitio donde se van a almacenar nuestros datos. El siguiente paso es crear un proceso ETL (Extraction, Transform and Load) con **Data Factory**.


En la barra de búsqueda escribimos **Data factories** y luego clickeamos en create. Nuevamente seleccionamos la subscripción, el grupo de recursos, la región y el nombre de nuestro data factory. Antes de dar clic en create, chuleamos la casilla Configure Git later de la pestaña Git configuration. Una vez creado, damos clic en Go to resource.

Una vez aquí, damos clic en Author & Monitor. 

Cuando estemos aquí, damos clic en Copy Data.


Escribimos el nombre de la tarea, y seleccionamos la frecuencia con la que se va a ejecutar.

En el 2do paso, creamos una nueva conexión dando clic en Create new connection. En la barra de búsqueda escribimos http, seleccionamos y damos clic en continuar. 

Damos un nombre a la conexión, copiamos la URL del archivo que queremos cargar en nuestro blob storage (para este ejemplo utilizamos los datos del covid alojados en los servidores de google cloud storage). En la opción Authentication type seleccionamos la opción Anonymous y damos clic en create. 

Una vez hecho lo anterior, nos debe salir lo siguiente:

Damos clic en next. La herramienta valida la conexión. 

En esta ventana seleccionamos la casilla Binary copy y damos clic en Next. 


En este punto debemos escoger la ruta destino de nuestro dataset. Para ello nuevamente creamos una conexión. Damos clic en Create new connection. 

En esta vectana seleccionamos Azure Blob Storage y damos clic en Continue. 


Seleccionamos el container donde queremos que se almacenen los datos y le damos un nombre al archivo. Clic en Next. 

Ignoramos la pestaña Settings y damos clic en Next. Luego nos muestra un resumen de la configuración que hemos hecho. Damos clic en next y el aplicativo valida todas las conexiones tal como lo podemos ver en la siguiente imagen. Damos clic en Finish. 



Luego en la ventana principal, damos clic en el botón el menú izquierdo, donde dice Monitor. Ahí podemos ver nuestro job y su estado. Esperamos a que esté en estado Succeeded. 

Volvemos al Blob Storage y al contenedor donde enviamos los datos y ahí los podemos ver. 



Una vez tengamos los datos en nuestro blob storage, procedemos a manipularlos desde DataBricks.

Iniciamos sesión en nuestro Databricks, creamos un clúster y esperamos que inicie. 


Una vez creado, creamos un nuevo notebook. En este notebook vamos a crear una referencia a nuestro blob storage de azure y a crear una base de datos .




Para crear un punto de montaje necesitamos ejecutar este código en una casilla de nuestro notebook. Recuerda modificar las variables con las de tu azure. 

```
container = 'your container name'
storagename = 'your storage name'
key = 'your azure credential'

dbutils.fs.mount(
  source = "wasbs://"+container+"@"+storagename+".blob.core.windows.net",
  mount_point = "/mnt/covid",
  extra_configs = {"fs.azure.account.key."+storagename+".blob.core.windows.net":""+key+""})
```

Con la siguiente línea de código leemos el dataframe.
```
df = spark.read.csv("/mnt/covid/datoscovid", header=True)
```


Con esta línea creamos una tabla temporal, lo que nos permite manipular el dataframe con sentencias SQL.
```
table_name = 'covid'
df.createOrReplaceTempView(table_name)
```


De esta forma creamos un DeltaLake, que es una *** con capacidades de CRUD. 
```
archivo = "/mnt/covid/delta/datoscovid/"
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(archivo)

sql = "CREATE TABLE covid USING DELTA LOCATION '/mnt/covid/delta/datoscovid/'"
spark.sql(sql)
```

### ¿Cuántos casos existen a nivel global?
```
casos_totales = spark.sql("""
                           SELECT count(1) as Casos 
                           FROM covid
                           """)
# Mostramos el resultado de la consulta
display(casos_totales)

# Guardamos el resultado en una tabla
casos_totales.write.saveAsTable('casos_totales')
```

### ¿Cuáles son las áreas o paises más afectados?
```
paises_afectados = sqlContext.sql("""
                           SELECT country_name, count(1) as Casos 
                           FROM covid 
                           GROUP BY country_name 
                           ORDER BY Casos DESC
                           """)

display(paises_afectados)
paises_afectados.write.saveAsTable('paises_afectados')
```


### Identificar los puntos críticos en estados unidos
```
puntos_criticos = sqlContext.sql("""
                           SELECT subregion1_name as subregion, count(1) as Casos 
                           FROM covid 
                           WHERE country_code='US' 
                           GROUP BY subregion 
                           ORDER BY Casos DESC 
                           """)
display(puntos_criticos)
puntos_criticos.write.saveAsTable('puntos_criticos')
```






