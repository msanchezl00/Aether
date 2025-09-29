# 🚀 Minimal-Crawler 🚀

[Go Producer] --> [Kafka Broker(s)] --> [Kafka Connect] --> [HDFS]

Este proyecto es un minimal crawler modular diseñado para capturar, procesar y almacenar datos de páginas web. El sistema está dividido en tres componentes principales: Fetcher, Parser y Storage, cada uno con responsabilidades claramente definidas. A continuación, encontrarás una explicación detallada de cómo funciona cada parte y cómo se integran entre sí.

## 🛠️ Arquitectura del Crawler

### Fetcher 🌐
El Fetcher es el componente encargado de hacer solicitudes HTTP para obtener el contenido de una página web. Su función principal es interactuar con servidores web, manejar respuestas y gestionar errores o redirecciones.

Funciones principales:

Recibe una URL como entrada.
Realiza la solicitud HTTP para obtener el contenido de la página.
Gestiona tiempos de espera, redirecciones, errores de servidor (404, 500) y restricciones como el archivo robots.txt.
Devuelve el contenido HTML bruto para ser procesado.

### Parser 🧩
El Parser se encarga de analizar el contenido obtenido por el Fetcher y extraer la información relevante. Se basa en herramientas con las que se puede navegar por el árbol DOM del HTML.

Funciones principales:

Recibe el contenido HTML como entrada.
Analiza y extrae los datos específicos que se necesitan (títulos, enlaces, imágenes, etc.).
Transforma los datos si es necesario (por ejemplo, limpieza de texto o normalización de URLs).
Devuelve la información en un formato estructurado, como un diccionario o JSON.

### Storage 💾
El Storage se encarga de almacenar los datos extraídos en un sistema persistente de indexers distibuido.

Funciones principales:

Recibe los datos estructurados del Parser.
Decide cómo almacenar los datos.
Guarda la información junto con metadatos (URL, fecha de captura, etc.).
Gestiona errores en el almacenamiento (por ejemplo, duplicados o inserciones fallidas, o problemas con los indexers).