# Covid Data Processing
Tutorial para cargar datos desde una fuente http a un Blob Storage de Azure, para posteriormente ser manipulados desde DataBricks y consumidos con Power Bi.

# Crear grupo de recursos
Creamos un grupo de recursos en azure para encapsular los servicios que utilicemos. Para ellos, buscamos el servicios **Resurce groups** y damos clic. 
Seleccionamos la subscripción, damos un nombre al grupo de recursos y seleccionamos la región.

Seleccionamos el grupo de recursos que hemos creado y añadimos un nuevo recurso. En la barra de búsqueda que aparece escribimos Blob y seleccionamos la opción que se llama: Storage account. Nos aparece la siguiente imagen y damos clic en crear.

Seleccionamos la subscripción, el grupo de recursos y escribimos los datos básicos de nuestros Blob Storage, tal como nombre, localización y tipo de almacenamiento, entre otros. Creamos y esperamos que el servicio esté desplegado. Damos clic en go to resource. 

Una vez aquí, damos clic en container y luego en crear. Asignamos un nombre y listo. Lo que hemos hecho hasta aquí es crear el sitio donde se van a almacenar nuestros datos. El siguiente paso es crear un proceso ETL (Extraction, Transform and Load) con **Data Factory**.

