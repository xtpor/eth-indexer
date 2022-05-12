
# Run development postgres

```
docker run -d --network development --rm --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=password postgres:14
```

# Run development adminer

```
docker run -d --network development --rm --name adminer -p 18080:8080 adminer
```