# Procesamiento en la nube con Azure y Databricks. 
## ICESI 2021-1 - Maestría en Ciencia de Datos

Tutorial para cargar datos desde una fuente Http a un Blob Storage de Azure, para posteriormente ser manipulados desde DataBricks y consumidos con Power Bi. Se pretende configurar todo el flujo de datos desde una fuente a otra utilizando herramientas cloud y al final, poder responder las siguientes preguntas sobre el COVID-19:
1. ¿Cuántos casos existen a nivel global?
1. ¿Cuáles son los paises más afectados?
1. Identificar las regiones o subregiones más críticas en Estados Unidos.
1. ¿Cuál es el índice de mortalidad?


## Objetivos
1. Crear un Blob Storage y su respectivo contenedor en Azure.  **(Almacenamiento)**
1. Crear y configurar la copia de datos desde una fuente Http (alojada en los servidores de Google Cloud) a el Blob Storage con Data Factory. **(ETL)**
1. Crear un punto de montaje al Blob Storage de Azure desde Databricks y realizar las consultas anteriormente propuestas. **(Exploración)**
1. Consumir los datos alojados en las tablas de Databricks desde PowerBI. **(Visualización)**


## Tabla de contenido 

- [Crear grupo de recursos](#crear-grupo-de-recursos)
- [Crear grupo de servicio de almacenamiento Blob Storage](#crear-grupo-de-servicio-de-almacenamiento-blob-storage)
- [Crear ETL con Data Factory](#crear-etl-con-data-factory)
- [Manipulación de los datos con Databricks](#manipulación-de-los-datos-con-databricks)
- [Consumo de los datos con PowerBi](#consumo-de-los-datos-con-powerbi)


## Crear grupo de recursos
Creamos un grupo de recursos en azure para encapsular los servicios que utilicemos. Para ellos, buscamos el servicios **Resurce groups** y damos clic. 

![Azure](/img/img%20(1).png)

Seleccionamos la subscripción, damos un nombre al grupo de recursos y seleccionamos la región.

![Azure](/img/img%20(2).png)

## Crear grupo de servicio de almacenamiento Blob Storage

Seleccionamos el grupo de recursos que hemos creado y añadimos un nuevo recurso. En la barra de búsqueda que aparece escribimos Blob y seleccionamos la opción que se llama: Storage account.
![Azure](/img/img%20(4).png)

Nos aparece la siguiente imagen y damos clic en crear.

![Azure](/img/img%20(5).png)

Seleccionamos la subscripción, el grupo de recursos y escribimos los datos básicos de nuestros Blob Storage, tal como nombre, localización y tipo de almacenamiento, entre otros. Creamos y esperamos que el servicio esté desplegado. Damos clic en go to resource. 

Una vez aquí, damos clic en container y luego en crear. Asignamos un nombre y listo. Lo que hemos hecho hasta aquí es crear el sitio donde se van a almacenar nuestros datos. El siguiente paso es crear un proceso ETL (Extraction, Transform and Load) con **Data Factory**.

![Azure](/img/img%20(7).png)

![Azure](/img/img%20(8).png)

## Crear ETL con Data Factory

En la barra de búsqueda escribimos **Data factories** y seleccionamos. Una vez cargue la página, clickeamos en create. Nuevamente seleccionamos la subscripción, el grupo de recursos, la región y el nombre de nuestro data factory. Antes de dar clic en create, chuleamos la casilla Configure Git later de la pestaña Git configuration. Una vez creado, damos clic en Go to resource.

![Azure](/img/img%20(9).png)

Una vez aquí, damos clic en Author & Monitor. 

![Azure](/img/img%20(12).png)

Cuando estemos aquí, damos clic en Copy Data.

![Azure](/img/img%20(13).png)

Escribimos el nombre de la tarea, y seleccionamos la frecuencia con la que se va a ejecutar.

![Azure](/img/img%20(14).png)

En el segundo paso, creamos una nueva conexión dando clic en Create new connection. En la barra de búsqueda escribimos Http, seleccionamos y damos clic en continuar. 

![Azure](/img/img%20(15).png)

![Azure](/img/img%20(16).png)

Damos un nombre a la conexión, copiamos la URL del archivo que queremos cargar en nuestro blob storage (para este ejemplo utilizamos los datos del covid alojados en los servidores de Google Cloud Storage). En la opción Authentication type seleccionamos la opción Anonymous y damos clic en create. 

![Azure](/img/img%20(17).png)

Una vez hecho lo anterior, nos debe salir lo siguiente:

![Azure](/img/img%20(18).png)

Damos clic en Next. La herramienta valida la conexión. 

En esta ventana seleccionamos la casilla Binary copy y damos clic en Next. 

![Azure](/img/img%20(19).png)

En este punto debemos escoger la ruta destino de nuestro dataset. Para ello nuevamente creamos una conexión. Damos clic en Create new connection. 

En esta ventana seleccionamos Azure Blob Storage y damos clic en Continue. 

![Azure](/img/img%20(20).png)

![Azure](/img/img%20(21).png)

Seleccionamos el container donde queremos que se almacenen los datos y le damos un nombre al archivo. Clic en Next. 

![Azure](/img/img%20(22).png)

Ignoramos la pestaña Settings y damos clic en Next. Luego nos muestra un resumen de la configuración que hemos hecho. Damos clic en next y el aplicativo valida todas las conexiones tal como lo podemos ver en la siguiente imagen. Damos clic en Finish. 

![Azure](/img/img%20(24).png)

Luego en la ventana principal, damos clic en el botón el menú izquierdo, donde dice Monitor. Ahí podemos ver nuestro job y su estado. Esperamos a que esté en estado Succeeded. 

![Azure](/img/img%20(26).png)

Volvemos al Blob Storage y al contenedor donde enviamos los datos y ahí los podemos ver. 

![Azure](/img/img%20(28).png)

Una vez tengamos los datos en nuestro blob storage, procedemos a manipularlos desde DataBricks.

## Manipulación de los datos con Databricks

El notebook de Databricks lo pueden consultar desde este enlace [link](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/8365969936768097/4092316647370143/1666563003672920/latest.html).

Iniciamos sesión en nuestro Databricks, creamos un clúster y esperamos que inicie. 

![Azure](/img/img%20(32).png)

![Azure](/img/img%20(31).png)

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

Si te fijas, te pide la credencial de Blob Storage de Azure. Esta la consigues en Blob Storage, en la opción del menu llamada Access keys.

![Azure](/img/img%20(33).png)


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

## Consumo de los datos con PowerBi

Para consumir los datos con PowerBi, requerimos conectarnos a nuestro Databricks por medio de JDBC-ODBC. Los datos que vamos a necesitar los encontramos en la siguiente ruta: databricks > Cluster > Configuration > JDBC/ODBC. En este ruta copiamos los siguientes datos:

* Server Hostname
* HTTP Path 

![Azure](/img/img%20(34).png)

Luego abrimos nuestro PowerBi Desktop. 

Creamos una nueva fuente de datos dando clic en Obtener datos.

![Azure](/img/img%20(35).png)

Buscamos Azure > Azure Databricks

![Azure](/img/img%20(36).png)

Pegamos las credenciales Server Hostname y HTTP Path. Damos clic en Aceptar y luego en iniciamos sesión con nuestras credenciales de Databrick (usuario y contraseña).

![Azure](/img/img%20(40).png)

Damos clic en Conectar.

Seleccionamos las tablas que deseamos importar y damos clic en Cargar.

![Azure](/img/img%20(37).png)

Veremos la siguiente ventana de carga.

![Azure](/img/img%20(38).png)

Una vez hayan cargado los datos, procedemos a graficar según las preguntas que queramos responder. 

![Azure](/img/img%20(39).png)







