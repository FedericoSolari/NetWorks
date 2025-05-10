# Instrucciones de uso:

## Levantar el server:
```
python3 src/start-server.py
```

## Client Upload
Para realizar un UPLOAD con stop and wait, levantar el client con:
```
python3 src/upload.py --src fiuba.jpg
```  

Para levantar con selective repeat:
```
python3 src/upload.py --src archivo.txt -pr sr
```

## Client Download
Para realizar un DOWNLOAD con stop and wait, levantar el client con: 
```
python3 src/download.py -d ../downloaded_file.txt -n archivo.txt
```

Para levantar con selective repeat:
```
python3 src/download.py -d ../downloaded_file.txt -pr sr -n archivo.txt
```

